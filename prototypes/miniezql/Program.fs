

open System
open TypeChecker
open PatternMatching
open Eval
open Ast
open Rewrite
open Types
open Primitives

let parse code =
    // let lexbuf = Lexing.from_text_reader Encoding.ASCII file
    let lexbuf = Lexing.from_string code
    try
        Parser.TopLevel Lexer.token lexbuf
    with e ->
        let pos = lexbuf.EndPos
        failwithf "Error near line %d, character %d\n" (pos.Line + 1) pos.Column




let typeCheck ast =
  let initialEnv = Map.of_list [ for f in Primitives.primitiveFunctions -> (f.Name, dg (TyPrimitive f.TypeCheck)) ]
  List.fold types initialEnv ast


let compile code =
    let ast = parse code |> rewrite1
    let types = typeCheck ast
    let ast' = rewrite2 ast
    ast'



let run code =
    let ast = compile code
    let initialEnv = Map.of_list [ for f in Primitives.primitiveFunctions -> (f.Name, VPrimitive f.Eval) ]
    List.fold (fun (env:Context) stmt ->
                 match stmt with
                 | Def (Identifier name, expr, None) ->
                     env.Add(name, eval env expr)
                 | Expr expr ->
                     try
                       printfn "%O" (eval env expr)
                     with err -> printfn "eval failed: %s" err.Message
                     env
                 | _ -> env)
              initialEnv ast |> ignore

(*
run "
let { a = a, b = b } = { a = 3, b = 7 } in a * b;;

let { a = a, b = { c = b } } = { a = 3, b = { c = 7 } } in a * b;;

let { a = 3, b = b } = { a = 3, b = 7 } in 3 * b;;

// Fails on runtime
let { a = 5, b = b } = { a = 3, b = 7 } in 3 * b;;

// Fails during typechecking
//let { a = 3, b = { c = b } } = { a = 3, b = 7 } in a * b;;

let { a = 3, b = _ } = { a = 3, b = { c = 7 } } in 3 * 7;;

let a = 2 in
let { a = a, b = 7 } = { a = a + 1, b = 7 } in
a * 7;;

let f = fun a b -> { a = a, b = b } in
let { a = a, b = b } = f 3 7 in a * 7;;

// This fails during typechecking.
//let f = fun a b -> { a = a, b = b, c = a + b } in
//let { a = a, b = b } = f 3 7 in a * 7;;

// Fails during typechecking
//let f = fun a b -> a + b in
//let { a = a, b = b } = f 3 7 in a * 7;;

let (a, b) = (3, 7) in a * b;;

let (a, { a = b }) = (3, { a = 7 }) in a * b;;

let fst = fun x ->
            let (a, _) = x in a
  in
    fst (3, 7) * 7;;


let x = (3, 7) in
let (a, _) = x in
let (_, b) = x in
a * b;;

let fst = fun x ->
            let (a, _) = x in a
  in fst (3, 7) * 7;;

let fst = fun x ->
            let (a, _) = x in a
  in (fst (fun x -> x, 7) 3) * 7;;

let fst = fun x ->
            let (a, z) = x in a in
let snd = fun x ->
            let (3, b) = x in b in
snd (3, 7) * fst (3, 7);;

// Fails
//let fst = fun x ->
//            let (a, z, _) = x in a in
//let snd = fun x ->
//            let (3, b) = x in b in
//snd (3, 7) * fst (3, 7);;

// Fails, because getA's parameter doesn't contain the field c
//let getA = fun x ->
//             let { a = a, b = b } = x in (a, b) in
//getA { a = true, b = false, c = true };;

let { a = (a, { a = b }, c), b = d } = { a = (1, { a = 2 }, 3), b = 4 } in (a, b, c, d);;

let f = fun x -> (1, { a = (2, 3) }) in
match f 3 with
  | (2, { a = a }) -> a
  | _ -> (0, 0)
end;;
"



run "

enum Blah =
  | Some of int
  | None of (bool, bool);;

enum State =
  | A of Blah
  | B of (int, { a:int, b:bool })
  | C of { a:int, b:(bool, bool) };;

match (2, B (3, { a = 60, b = false })) with
//match (2, A (None (true, false))) with
 | (_, A opt) ->
     match opt with
     | Some v                   -> print (\"A Some \" + v)
     | None (true, b)           -> print (\"A None true \" + b)
     | None (a, b)              -> print (\"A None: \" + a + \" \" + b)
     end
 | (_, A (Some a))              -> print (\"A \" + a)
 | (a, A (None (true, b)))      -> print (\"A None: \" + a + \" \" + b)
 | (_, B (_, { a = a, b = b })) -> print (\"B \" + a + \" \" + b)
 | (_, C { a = a, b = b })      -> print (\"C \" + a + \" \" + b)
 | _                            -> print \"else\"
end;;

match 1 with
| 1, 0 > 1 -> 2
| a, 1 > 0 -> a
| _ -> 3
end;;

let f = fun (A (Some v)) -> v in
f (A (Some 3));;
"
*)

(*
run "
let x = fun y z -> z * y + 5
 in x 10 10;;

let fact = fun n ->
             if n == 0
               then 1
               else n * fact (n - 1)
  in
    fact;;

({ a = fun x -> x + \"damn\" }.a) \"blah\";;

let id = fun x -> x
in id 5; id 6.0;;

(fun f x -> let g = f in g x)
  (fun x -> { a = x.a * 2 })
  { a = 5, b = 50 };;
"
*)

run "
tempReadings = stream of { timestamp:int, roomId:int, temperature:int };;

define sum n = n.sum();;

sum 5;;

sum 5.0;;
"

Console.ReadLine() |> ignore

