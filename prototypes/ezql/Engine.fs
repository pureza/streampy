#light

open Scheduler
open Clock
open Ast
open Types
open Oper
open Graph
open Dataflow

let addEvent stream event =
  spread [(stream, ([(0, [Added (VEvent event)])]))]

let parse code =
    // let lexbuf = Lexing.from_text_reader Encoding.ASCII file
    let lexbuf = Lexing.from_string code
    try
        Parser.start Lexer.token lexbuf
    with e ->
        let pos = lexbuf.EndPos
        failwithf "Error near line %d, character %d\n" (pos.Line + 1) pos.Column


let compile code =
    let ast = parse code
    match ast with
    | Prog stmts ->
        let g = Graph.empty
        let env = Map.empty
        let roots = []
        let env', g', roots' = List.fold_left dataflow (env, g, roots) stmts

        //Graph.Viewer.display g' (fun v info -> info.Uid)
        let rootUids = List.map (fun name -> env'.[name].Uid) roots'
        let operators = Dataflow.makeOperNetwork g' rootUids id
        let rootStreams = List.fold_left (fun acc x -> Map.add x (fun ev -> addEvent operators.[env'.[x].Uid] ev) acc) Map.empty roots'
        let declaredOps = Map.fold_left (fun acc k v -> Map.add k operators.[v.Uid] acc) Map.empty env'
        rootStreams, declaredOps

let mainLoop () =
    let virtualClock = Scheduler.clock () :?> VirtualClock
    while virtualClock.HasNext () do
        virtualClock.Step ()

let reset () =
    Scheduler.reset ()

let now () = Scheduler.clock().Now
