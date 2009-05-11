#light

open Ast
open Types
open Eval
open Scheduler

(* A stream: propagates received events *)
let makeStream uid prio parents =
  let eval = fun op inputs -> match inputs with
                              | [[Added (VEvent ev)] as changes] -> Some (op.Children, changes)
                              | _ -> failwith "stream: Invalid arguments!"

  Operator.Build(uid, prio, eval, parents)

(* A simple window - does not schedule expiration of events. *)
let makeSimpleWindow uid prio parents =
  let eval = fun op inputs -> match inputs with
                              | changes::_ -> Some (op.Children, changes)
                              | _ -> failwith "Simple window: Invalid arguments!"

  Operator.Build(uid, prio, eval, parents)
  
(* A timed window *)
let makeWindow duration uid prio parents =
  let eval = fun op inputs -> match inputs with
                              | [[Added (VEvent ev)] as changes] ->
                                  Scheduler.scheduleOffset duration (List.of_seq op.Children, [Expired (VEvent ev)])
                                  Some (op.Children, changes)
                              | _ -> failwith "timed window: Invalid arguments!"

  Operator.Build(uid, prio, eval, parents)

(* Generic operator builder for stream.where(), stream.select() and when() 
   All these operators receive a lambda expression that is to be executed
   when an event arrives. How to handle the result of this evaluation
   is specific to each operator, and is abstracted by the resultHandler
   function.
 *)
let makeEvalOnAdd resultHandler eventHandler uid prio parents =
    let expr = FuncCall (eventHandler, [Id (Identifier "ev")])
    let eval = fun op inputs ->
                 let env = getOperEnvValues op
                 match inputs with
                 | [Added (VEvent ev)]::_ ->
                     let env' = Map.add "ev" (VEvent ev) env
                     resultHandler op inputs ev (eval env' expr)
                 | []::_ -> None // Ignore changes to the handler dependencies
                 | _ -> failwithf "Wrong number of arguments! %A" inputs

    Operator.Build(uid, prio, eval, parents)


(* Where: propagates events that pass a given predicate *)
let makeWhere = makeEvalOnAdd (fun op inputs ev result ->
                                 match result with
                                 | VBool true -> Some (op.Children, inputs.Head)
                                 | VBool false -> None
                                 | _ -> failwith "Predicate was supposed to return VBool")

(* Select: projects an event *)
let makeSelect = makeEvalOnAdd (fun op inputs ev result ->
                                  let ev' = match result with
                                            | VRecord map -> Event (ev.Timestamp, recordToEvent map)
                                            | _ -> failwithf "select should return a record"
                                  Some (op.Children, [Added (VEvent ev')]))

(* When *)
let makeWhen = makeEvalOnAdd (fun op inputs ev result -> Some (op.Children, [Added result]))


(* A timed window for dynamic values *)
let makeDynValWindow duration uid prio parents =
  let eval = fun (op:Operator) inputs ->
               match inputs with
               | [[Added something] as changes] ->
                   if op.Value <> VNull
                     then Scheduler.scheduleOffset duration (List.of_seq op.Children, [Expired op.Value])
                   op.Value <- something
                   Some (op.Children, changes)
               | _ -> failwith "timed window: Wrong number of arguments!"

  Operator.Build(uid, prio, eval, parents)

(* Evaluator: this operator evaluates some expression and records its result *)
let makeEvaluator expr uid prio parents =
  let operEval = fun oper allChanges ->
                   let env = Map.of_list [ for p in oper.Parents -> (p.Uid, p.Value) ]
                   let result = eval env expr
                   setValueAndGetChanges oper result

  let initialContents = try
                          let env = Map.of_list [ for p in parents -> (p.Uid, p.Value) ]
                          eval env expr
                        with
                          | err -> VNull
  Operator.Build(uid, prio, operEval, parents, contents = initialContents)


let makeRecord (fields:list<string * Operator>) uid prio parents =
  let result = fields |> List.map (fun (f, _) -> (VString f, ref VNull)) |> Map.of_list
  assert (parents = (List.map snd fields))

  let recordOp = Operator.Build(uid, prio,
                   (fun oper allChanges ->
                      let recordChanges =
                        allChanges
                          |> List.mapi (fun i changes ->
                                          match changes with
                                          | x::xs -> let field, parent = fields.[i]
                                                     result.[VString field] := parent.Value
                                                     [RecordDiff (VString field, changes)]
                                          | _ -> [])
                          |> List.concat
                      Some (oper.Children, recordChanges)),
                   parents, contents = VRecord result)

  for field, op in fields do
    result.[VString field] := op.Value

  recordOp


(* Converts a dynamic value into a stream of changes to the value *)
let makeToStream uid prio parents =
  let eval = fun (op:Operator) inputs ->
               let value = op.Parents.[0].Value
               let ev = Event (Scheduler.clock().Now, Map.of_list ["value", value])
               Some (op.Children, [Added (VEvent ev)])

  Operator.Build(uid, prio, eval, parents)
