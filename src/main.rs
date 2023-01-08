use anyhow::Result;
use axum::{response::IntoResponse, routing::post, Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{mpsc::Sender, oneshot},
};
use tracing::{info, warn};
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

type ProcessId = usize;
type ProcessAddress = &'static str;

#[derive(Debug)]
enum Message {
    Election {
        request: ElectionRequest,
        tx_answer: oneshot::Sender<ElectionResponse>,
    },
    Victory {
        request: VictoryRequest,
        tx_answer: oneshot::Sender<VictoryResponse>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct ElectionRequest {
    process_id: ProcessId,
}

#[derive(Debug, Serialize, Deserialize)]
struct ElectionResponse {
    process_id: ProcessId,
}

#[derive(Debug, Serialize, Deserialize)]
struct VictoryRequest {
    process_id: ProcessId,
}

#[derive(Debug, Serialize, Deserialize)]
struct VictoryResponse {
    process_id: ProcessId,
}

#[derive(Debug)]
enum State {
    /// Process is the leader.
    Leader,
    /// Process is just a follower.
    Follower,
    /// Process is trying to become the leader.
    Candidate,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_env("RUST_LOG"))
        .init();

    let process_id: usize = std::env::var("ID")
        .expect("process id is required: ID=<usize>")
        .parse()
        .expect("process id must be a number");

    let processes: HashMap<ProcessId, &str> = HashMap::from([
        (0, "127.0.0.1:3000"),
        (1, "127.0.0.1:3001"),
        (2, "127.0.0.1:30002"),
        (3, "127.0.0.1:3003"),
        (4, "127.0.0.1:3004"),
    ]);

    let process_addr = processes.get(&process_id).unwrap();

    let (tx_message, mut rx_message) = tokio::sync::mpsc::channel::<Message>(100);

    // build our application with a single route
    let app = Router::new()
        .route("/election", post(election))
        .route("/victory", post(victory))
        .layer(Extension(processes.clone()))
        .layer(Extension(tx_message))
        .layer(Extension(process_id));

    info!("starting {process_id} on address {process_addr}");

    tokio::spawn(async {
        axum::Server::bind(&process_addr.parse().unwrap())
            .serve(app.into_make_service())
            .await
            .expect("unable to start http server");
    });

    let mut leader_heartbeat_interval = tokio::time::interval(Duration::from_secs(5));
    let leader_heartbeat_timeout = Duration::from_secs(10);

    let mut current_leader_id = None;
    let mut last_leader_heartbeat_at = Instant::now();

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("unable to build http client");

    let mut state = State::Follower;

    info!("starting process {process_id}");

    loop {
        match state {
            State::Leader => {
                info!("process {process_id} is leader");
                select! {
                    _ = leader_heartbeat_interval.tick() => {
                        if let Err(err) = leader_heartbeats(process_id, &processes, &http_client).await {
                            warn!("error sending leader heartbeats. error={:?}",err)
                        }
                    }
                    message = rx_message.recv() => {
                        let message = message.unwrap();

                        match message {
                            Message::Election { request, tx_answer}  => {
                                if request.process_id > process_id {
                                    state = State::Follower;
                                }

                                // TODO: why does this crash sometimes? (maybe the receiver has been dropped)
                                if let Err(error) = tx_answer.send(ElectionResponse { process_id }) {
                                    warn!(?error, "unable to send election response");
                                };
                             },
                            Message::Victory { request, tx_answer}  => {
                                if request.process_id > process_id {
                                    state = State::Follower;
                                    last_leader_heartbeat_at = Instant::now();
                                }

                                tx_answer.send(VictoryResponse { process_id }).expect("unable to send victory response");
                            }
                        }
                    }
                }
            }
            State::Follower => {
                info!("process {process_id} is a follower");
                // If this process has the highest id, try to become the leader.
                if process_id == processes.keys().max().cloned().unwrap()
                    || current_leader_id.is_some() && current_leader_id.unwrap() < process_id
                {
                    info!(
                        "process_id={process_id} process id is higher than the current leader id"
                    );
                    match broadcast_victory(process_id, &processes, &http_client).await {
                        // Got not responses from other processes, this process is the only process alive.
                        None => {
                            state = State::Leader;
                        }
                        Some(response) => {
                            // We found a process with a higher id, so there's no way this proccess can be the leader.
                            state = if response.process_id > process_id {
                                State::Follower
                            } else {
                                State::Leader
                            }
                        }
                    }

                    continue;
                }

                // Process is not the process with the highest id.
                select! {
                    message = rx_message.recv() => {
                        let message = message.unwrap();
                        match message {
                            Message::Election { request, tx_answer}  => {
                                if request.process_id < process_id {
                                    state = State::Candidate;
                                }

                                tx_answer.send(ElectionResponse { process_id }).expect("unable to send election response");
                            },
                            Message::Victory { request, tx_answer}  => {
                                if request.process_id < process_id {
                                    state = State::Candidate;
                                } else {
                                    last_leader_heartbeat_at = Instant::now();
                                    current_leader_id = Some(request.process_id);
                                }

                                tx_answer.send(VictoryResponse { process_id }).expect("unable to send victory response");
                            }
                        }
                    },
                    _ = tokio::time::sleep(leader_heartbeat_timeout) => {
                        // Leader is dead, start election.
                        if last_leader_heartbeat_at.elapsed() > leader_heartbeat_timeout {
                            state = State::Candidate;
                        }
                    }
                }
            }
            State::Candidate => {
                info!("process {process_id} is a candidate");

                // If this process has the highest id, try to become the leader.
                if process_id == processes.keys().max().cloned().unwrap()
                    || current_leader_id.is_some() && current_leader_id.unwrap() < process_id
                {
                    info!("process {process_id} has a id higher than the current leader: {current_leader_id:?}");

                    match broadcast_victory(process_id, &processes, &http_client).await {
                        // Got not responses from other processes, this process is the only process alive.
                        None => {
                            info!("process {process_id} became leader after victory broadcast because no one answered");
                            state = State::Leader;
                        }
                        Some(response) => {
                            // We found a process with a higher id, so there's no way this proccess can be the leader.
                            state = if response.process_id > process_id {
                                info!("process {process_id} found a process with a higher id so it becomes a follower");
                                State::Follower
                            } else {
                                info!("process {process_id} became leader after victory broadcast");
                                State::Leader
                            }
                        }
                    }

                    continue;
                }

                info!("process {process_id} doesn't have the highest id, so it starts an election");

                // Process doesn't have the highest id, so it starts an election.
                match broadcast_election(process_id, &processes, &http_client).await {
                    // Every other process is dead.
                    None => {
                        info!("process {process_id} no one answered election broadcast, will broadcast victory");
                    }
                    Some(response) => {
                        if response.process_id > process_id {
                            info!("process {process_id} became follower because it found a process with a higher id after election broadcast");
                            state = State::Follower;
                            continue;
                        }
                    }
                }

                state = broadcast_victory(process_id, &processes, &http_client)
                    .await
                    .map(|response| {
                        if response.process_id > process_id {
                            State::Follower
                        } else {
                            State::Leader
                        }
                    })
                    .unwrap_or(State::Leader);

                info!("process {process_id} became {state:?} after election followed by victory broadcast");
            }
        }
    }
}

/// Called by processes participating in an election.
#[axum_macros::debug_handler]
async fn election(
    Extension(tx): Extension<Sender<Message>>,
    Json(request): Json<ElectionRequest>,
) -> impl IntoResponse {
    let (tx_answer, rx) = tokio::sync::oneshot::channel();
    match tx.send(Message::Election { request, tx_answer }).await {
        Ok(()) => {
            let response = rx.await.unwrap();
            Ok(Json(response))
        }
        Err(err) => {
            warn!("unable to send election message: {:?}", err);
            Err(err.to_string())
        }
    }
}

/// Called by the process that is the leader.
#[axum_macros::debug_handler]
async fn victory(
    Extension(tx): Extension<Sender<Message>>,
    Json(request): Json<VictoryRequest>,
) -> impl IntoResponse {
    let (tx_answer, rx) = tokio::sync::oneshot::channel();
    match tx.send(Message::Victory { request, tx_answer }).await {
        Ok(()) => {
            let response = rx.await.unwrap();
            Ok(Json(response))
        }
        Err(err) => {
            warn!("unable to process victory message: {:?}", err);
            Err(err.to_string())
        }
    }
}

async fn broadcast_victory(
    process_id: ProcessId,
    processes: &HashMap<ProcessId, ProcessAddress>,
    http_client: &reqwest::Client,
) -> Option<VictoryResponse> {
    let mut response_with_highest_proc_id: Option<VictoryResponse> = None;

    for (proc_id, addr) in processes.iter() {
        if *proc_id != process_id {
            match http_client
                .post(format!("http://{addr}/victory"))
                .json(&VictoryRequest { process_id })
                .send()
                .await
            {
                Err(err) => {
                    warn!(
                        "unable to send victory message. error={:?} proc_id={} addr={}",
                        err, proc_id, addr
                    );
                }
                Ok(response) => {
                    let victory_response = response.json::<VictoryResponse>().await.unwrap();

                    let found_process_with_higher_id = response_with_highest_proc_id
                        .as_ref()
                        .map(|r| r.process_id < victory_response.process_id)
                        .unwrap_or(false);

                    if found_process_with_higher_id || response_with_highest_proc_id.is_none() {
                        response_with_highest_proc_id = Some(victory_response);
                    }
                }
            }
        }
    }

    response_with_highest_proc_id
}

async fn broadcast_election(
    process_id: ProcessId,
    processes: &HashMap<ProcessId, ProcessAddress>,
    http_client: &reqwest::Client,
) -> Option<ElectionResponse> {
    let mut response_with_highest_proc_id: Option<ElectionResponse> = None;

    for (proc_id, addr) in processes.iter() {
        if *proc_id > process_id {
            info!("broadcasting election to process process_id={process_id} proc_id={proc_id}");

            let url = format!("http://{addr}/election");

            match http_client
                .post(&url)
                .json(&ElectionRequest { process_id })
                .send()
                .await
            {
                Err(err) => {
                    warn!(
                        "unable to broadcast election message. error={:?} proc_id={} addr={} url={}",
                        err, proc_id, addr, url
                    );
                }
                Ok(response) => {
                    let election_response = response.json::<ElectionResponse>().await.unwrap();

                    let found_process_with_higher_id = response_with_highest_proc_id
                        .as_ref()
                        .map(|r| r.process_id < election_response.process_id)
                        .unwrap_or(false);

                    if found_process_with_higher_id || response_with_highest_proc_id.is_none() {
                        response_with_highest_proc_id = Some(election_response);
                    }
                }
            }
        }
    }

    response_with_highest_proc_id
}

async fn leader_heartbeats(
    process_id: ProcessId,
    processes: &HashMap<ProcessId, ProcessAddress>,
    http_client: &reqwest::Client,
) -> Result<()> {
    // TODO: could be done in parallel
    for (proc_id, addr) in processes.iter() {
        if *proc_id != process_id {
            http_client
                .post(format!("http://{addr}/victory"))
                .json(&VictoryRequest { process_id })
                .send()
                .await?;
        }
    }

    Ok(())
}
