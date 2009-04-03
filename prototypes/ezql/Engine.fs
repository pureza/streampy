#light

open Scheduler
open Clock
open Ast
open Types


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

    // Initial environment
   // let env = Map.of_list [("stream", VType "stream")]
    //()
 //   match ast with
 //   | Prog stmts -> List.fold_left eval env stmts  
    ast
   
let mainLoop () = 
    let virtualClock = Scheduler.clock () :?> VirtualClock
    while virtualClock.HasNext () do
        virtualClock.Step ()
              
let reset () =
    Scheduler.reset ()
    
let now () = Scheduler.clock().Now