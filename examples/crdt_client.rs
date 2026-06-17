use clap::{Parser, ValueEnum};
use clap_repl::reedline::{DefaultPrompt, Reedline, Signal};
use crdt::client::{RemoteStore, SharedVariable};
use crdt::crdt::CrdtKind;
use std::error::Error;

#[derive(Debug, Clone, ValueEnum)]
pub enum CrdtType {
    GCounter,
    LWWSet,
}

impl From<CrdtType> for CrdtKind {
    fn from(t: CrdtType) -> Self {
        match t {
            CrdtType::GCounter => CrdtKind::GCounter,
            CrdtType::LWWSet => CrdtKind::LWWSet,
        }
    }
}

#[derive(Debug, Parser)]
#[command(no_binary_name = true)]
pub enum GlobalCommand {
    New { name: String, crdt_type: CrdtType },
    Vars,
    Connect,
    Disconnect,
    Quit,
}

#[derive(Debug, Parser)]
#[command(no_binary_name = true)]
pub enum GCounterCmd {
    Inc,
    Value,
}

#[derive(Debug, Parser)]
#[command(no_binary_name = true)]
pub enum LWWSetCmd {
    Add { element: String },
    Remove { element: String },
    Value,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server address to connect to
    #[arg(short, long, default_value = "http://[::1]:50051")]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let store = RemoteStore::new();
    if let Err(_e) = store.connect(&args.addr).await {
        eprintln!("Initial connection failed. You can connect later using the 'Connect' command.");
    }

    let mut line_editor = Reedline::create();
    let prompt = DefaultPrompt::default(); // TODO: Can we customize the prompt?

    loop {
        let sig = line_editor.read_line(&prompt);

        match sig {
            Ok(Signal::Success(buffer)) => {
                let tokens: Vec<&str> = buffer.split_whitespace().collect();
                if tokens.is_empty() {
                    continue;
                }

                if let Ok(global_cmd) = GlobalCommand::try_parse_from(&tokens) {
                    match global_cmd {
                        GlobalCommand::New { name, crdt_type } => {
                            if let Err(e) = store.create(&name, crdt_type.into()).await {
                                eprintln!("Failed to create variable: {:?}", e);
                            }
                        }
                        GlobalCommand::Vars => {
                            for (name, kind) in store.list() {
                                println!("{}: {:?}", name, kind);
                            }
                        }
                        GlobalCommand::Connect => {
                            if !store.is_connected() {
                                if let Err(e) = store.connect(&args.addr).await {
                                    eprintln!("Connection failed: {:?}", e);
                                } else {
                                    println!("Connected successfully.");
                                }
                            }
                        }
                        GlobalCommand::Disconnect => {
                            store.disconnect();
                            println!("Disconnected.");
                        }
                        GlobalCommand::Quit => break,
                    }
                    continue;
                }

                // Here we knot it wasn't a system prompt.
                // Try to parse it as a variable operation.

                let identifier = tokens[0];

                if let Some(var) = store.get(identifier) {
                    let args = &tokens[1..];

                    match var {
                        SharedVariable::Counter(counter) => {
                            match GCounterCmd::try_parse_from(args) {
                                Ok(GCounterCmd::Inc) => counter.inc(1),
                                Ok(GCounterCmd::Value) => {
                                    println!("Counter Value: {}", counter.value())
                                }
                                Err(e) => {
                                    let _ = e.print();
                                }
                            }
                        }
                        SharedVariable::Set(set) => match LWWSetCmd::try_parse_from(args) {
                            Ok(LWWSetCmd::Add { element }) => set.add(element),
                            Ok(LWWSetCmd::Remove { element }) => set.remove(element),
                            Ok(LWWSetCmd::Value) => {
                                println!("Set Members: {:#?}", set.members())
                            }
                            Err(e) => {
                                let _ = e.print();
                            }
                        },
                    }
                } else {
                    eprintln!("unknown command or variable")
                }
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => break,
            Err(err) => {
                eprintln!("error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
