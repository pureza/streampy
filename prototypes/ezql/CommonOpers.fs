#light

open Ast
open Types
open Eval
open Scheduler

(* A stream: propagates received events *)
let makeStream uid prio parents =
  let eval = fun op inputs -> match inputs with
                              | [[Added (VEvent ev)] as changes] -> Some (op.Children, changes)
                              | _ -> failwith "stream: Wrong number of arguments!"

  Operator.Build(uid, prio, eval, parents)

(* A simple window - does not schedule expiration of events. *)
let makeSimpleWindow uid prio parents =
  let eval = fun op inputs -> match inputs with
                              | changes::_ ->
                                  Some (op.Children, changes)
                              | _ -> failwith "Simple window: Wrong number of arguments!"

  Operator.Build(uid, prio, eval, parents)
  
(* A timed window *)
let makeWindow duration uid prio parents =
  let eval = fun op inputs -> match inputs with
                              | [[Added (VEvent ev)] as changes] ->
                                  Scheduler.scheduleOffset duration (List.of_seq op.Children, [Expired (VEvent ev)])
                                  Some (op.Children, changes)
                              | _ -> failwith "timed window: Wrong number of arguments!"

  Operator.Build(uid, prio, eval, parents)

(* Where: propagates events that pass a given predicate *)
let makeWhere predLambda uid prio parents =
    let expr = FuncCall (predLambda, [Id (Identifier "ev")])
    let eval = fun op inputs ->
                 let env = getOperEnvValues op
                 match inputs with
                 | [Added (VEvent ev)]::_ ->
                     let env' = Map.add "ev" (VEvent ev) env
                     match eval env' expr with
                     | VBool true -> Some (op.Children, inputs.Head)
                     | VBool false -> None
                     | _ -> failwith "Predicate was supposed to return VBool"
                 | []::_ -> None // Ignore changes to the predicate dependencies
                 | _ -> failwithf "where: Wrong number of arguments! %A" inputs

    Operator.Build(uid, prio, eval, parents)


(* Select: projects an event *)
let makeSelect predLambda uid prio parents =
    let expr = FuncCall (predLambda, [Id (Identifier "ev")])
    let eval = fun op inputs ->
                 let env = getOperEnvValues op
                 match inputs with
                 | [Added (VEvent ev)]::_ ->
                     let env' = Map.add "ev" (VEvent ev) env
                     let ev' = match eval env' expr with
                               | VRecord map -> Event (ev.Timestamp, recordToEvent map)
                               | _ -> failwithf "select should return a record"
                     Some (op.Children, [Added (VEvent ev')])
                 | []::_ -> None // Ignore changes to the predicate dependencies
                 | _ -> failwithf "where: Wrong number of arguments! %A" inputs

    Operator.Build(uid, prio, eval, parents)


(* Generic dynamic value: records any value received *)
let makeDynVal uid prio parents =
  let eval = fun op inputs -> match inputs with
                              | [[Added v]] -> setValueAndGetChanges op v
                              | _ -> failwith "dynVal: Wrong number of arguments! %A" inputs

  Operator.Build(uid, prio, eval, parents)

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


(* When *)
let makeWhen handler uid prio parents =
    let expr = FuncCall (handler, [Id (Identifier "ev")])
    let eval = fun op inputs ->
                 let env = getOperEnvValues op
                 match inputs with
                 | [Added (VEvent ev)]::_ ->
                     let env' = Map.add "ev" (VEvent ev) env
                     eval env' expr |> ignore
                     None
                 | []::_ -> None // Ignore changes to the predicate dependencies
                 | _ -> failwithf "where: Wrong number of arguments! %A" inputs

    Operator.Build(uid, prio, eval, parents)


(* Converts a dynamic value into a stream of changes to the value *)
let makeToStream uid prio parents =
  let eval = fun (op:Operator) inputs ->
               match inputs with
               | [[Added something]] ->
                   let ev = Event ((Scheduler.clock()).Now, Map.of_list ["value", something])
                   Some (op.Children, [Added (VEvent ev)])
               | _ -> failwithf "toStream: Invalid arguments: %A" inputs

  Operator.Build(uid, prio, eval, parents)


(* Projects a field out of a record *)
let makeProjector field uid prio parents =
  let eval = fun (op:Operator) inputs ->
               let fieldChanges = List.first (fun diff ->
                                                match diff with
                                                | RecordDiff (field', chg) when (VString field) = field' -> Some chg
                                                | _ -> None)
                                             (List.hd inputs)
               match fieldChanges with
               | Some changes -> let record = op.Parents.[0].Value
                                 match record with
                                 | VRecord record' -> op.Value <- !record'.[VString field]
                                 | _ -> failwith "What? The parent of a projector is not a record?"
                                 Some (op.Children, changes)
               | None -> None


  Operator.Build(uid, prio, eval, parents)