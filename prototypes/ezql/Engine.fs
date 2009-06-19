#light

open Scheduler
open Clock
open Ast
open Types
open TypeChecker
open Rewrite
open Oper
open Graph
open Dataflow

let parse code =
    // let lexbuf = Lexing.from_text_reader Encoding.ASCII file
    let lexbuf = Lexing.from_string code
    try
        Parser.TopLevel Lexer.token lexbuf
    with e ->
        let pos = lexbuf.EndPos
        failwithf "Error near line %d, character %d\n" (pos.Line + 1) pos.Column


let typeCheck ast = List.fold types Map.empty ast

let dataflowAnalysis types ast =
  let g = Graph.empty
  let env = Map.empty
  let roots = []
  let env', types', g', roots' = List.fold dataflow (env, types, g, roots) ast

  //Graph.Viewer.display graph (fun v info -> (sprintf "%s (%s)" info.Name v))
  let operators = Dataflow.makeOperNetwork g' (Graph.nodes g') id Map.empty
  Map.fold_left (fun acc k v -> Map.add k operators.[v.Uid] acc) Map.empty env'


let compile code =
    let ast = parse code
    let types = typeCheck ast
    let ast' = rewrite types ast
    dataflowAnalysis types ast'

let mainLoop () =
    let virtualClock = Scheduler.clock () :?> VirtualClock
    while virtualClock.HasNext () do
        virtualClock.Step ()

let reset () = Scheduler.reset ()
              // theRootOps := Map.empty
               theForwardDeps.Reset ()

let now () = Scheduler.clock().Now
