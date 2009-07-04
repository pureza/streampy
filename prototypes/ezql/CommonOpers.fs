#light

open System.Collections.Generic
open Extensions
open Extensions.DateTimeExtensions
open Ast
open Util
open Types
open Eval
open Scheduler
open Oper

(* A stream: propagates received events *)
let makeStream (uid, prio, parents, context) =
  let eval = fun ((op:Operator), inputs) ->
               match inputs with
               | [[Added (VRecord _ as ev)] as changes] ->
                   op.Value <- ev
                   Some (op.Children, changes)
               | _ -> failwith "stream: Invalid arguments!"

  Operator.Build(uid, prio, eval, parents, context)

(* A simple window - does not schedule expiration of events. *)
let makeSimpleWindow (uid, prio, parents, context) =
  let eval = fun (op, inputs) ->
               match inputs with
               | changes::_ -> Some (op.Children, changes)
               | _ -> failwith "Simple window: Invalid arguments!"

  Operator.Build(uid, prio, eval, parents, context)
  
(* A timed window *)
let makeWindow duration (uid, prio, parents, context) =
  let contents = ref []

  let eval = fun (op, inputs) ->
               let parentChanges = List.hd inputs
               for change in parentChanges do
                 match change with
                 | Added ev -> contents := (!contents) @ [ev]
                               Scheduler.scheduleOffset duration (List.of_seq [op, 0, id], [Expired ev])
                 | Expired ev -> assert ((!contents).Head = ev)
                                 contents := (!contents).Tail
                 | _ -> failwithf "Invalid changes to window: %A" parentChanges
               op.Value <- VWindow !contents
               Some (op.Children, parentChanges)

  Operator.Build(uid, prio, eval, parents, context, contents = VWindow [])

(* Generic operator builder for stream.where(), stream.select() and when() 
   All these operators receive a lambda expression that is to be executed
   when an event arrives. How to handle the result of this evaluation
   is specific to each operator, and is abstracted by the resultHandler
   function. *)
let makeEvalOnAdd resultHandler eventHandler (uid, prio, parents, context) : Operator =
    let expr = FuncCall (eventHandler, [Id (Identifier "ev")])
    let eval = fun (op, inputs) ->
                 let env = getOperEnvValues op
                 match inputs with
                 | [Added ev]::_ ->
                     let env' = Map.add "ev" ev env
                     resultHandler op inputs ev (eval env' expr)
                 | []::_ -> None // Ignore changes to the handler dependencies
                 | _ -> failwithf "Wrong number of arguments! %A" inputs

    Operator.Build(uid, prio, eval, parents, context)


(* Where: propagates events that pass a given predicate *)
let makeWhere = makeEvalOnAdd (fun op inputs ev result ->
                                 match result with
                                 | VBool true ->
                                     op.Value <- ev
                                     Some (op.Children, inputs.Head)
                                 | VBool false -> None
                                 | _ -> failwith "Predicate was supposed to return VBool")

(* Select: projects an event *)
let makeSelect = makeEvalOnAdd (fun op inputs ev result ->
                                  let timestamp = eventTimestamp ev
                                  let ev' = match result with
                                            | VRecord fields -> VRecord (Map.add (VString "timestamp") timestamp fields)
                                            | _ -> failwithf "select should return a record"
                                  op.Value <- ev'
                                  Some (op.Children, [Added ev']))

(* When *)
let makeWhen2 = makeEvalOnAdd (fun op inputs ev result ->
                                setValueAndGetChanges op result)

(* When is composed of two operators: the first receives inputs and postpones their
   evaluation which will take place at the second operator. *)
let makeWhen eventHandler (uid, prio, parents, context) =
  let expr = FuncCall (eventHandler, [Id (Identifier "ev")])
  
  let rec opTop = Operator.Build(uid, prio,
                    (fun (op, inputs) -> 
                       match List.hd inputs with
                       | [] -> ()
                       | ev -> Scheduler.scheduleOffset 0 (List.of_seq [opBottom, 0, id], ev)
                       None),
                    parents, context)
 
                  
  and opBottom = Operator.Build(uid + "_bottom", Priority.add prio (Priority.of_list [9]),
                   (fun (op, inputs) ->
                      let env = getOperEnvValues opTop
                      match inputs with
                      | [Added ev]::_ ->
                          let env' = Map.add "ev" ev env
                          let result = eval env' expr
                          setValueAndGetChanges op result
                      | _ -> failwithf "Wrong number of arguments! %A" inputs),
                   [opTop], context) 
                              
  { opTop with
        Children = opBottom.Children;
        Contents = opBottom.Contents;
        AllChanges = opBottom.AllChanges }

(* A timed window for dynamic values *)
let makeDynValWindow duration (uid, prio, parents, context) =
  let contents = ref []

  let eval = fun (op, inputs) ->
               let parentChanges = List.hd inputs
               for change in parentChanges do
                 match change with
                 | Added v -> let contents' = !contents
                              if contents'.Length > 0
                                then let current = contents'.[contents'.Length - 1]
                                     Scheduler.scheduleOffset duration (List.of_seq [op, 0, id], [Expired current])
                              contents := contents' @ [v]
                 | Expired v -> assert ((!contents).Head = v)
                                contents := (!contents).Tail
                 | _ -> failwithf "Invalid changes to window: %A" parentChanges
               op.Value <- VWindow !contents
               Some (op.Children, parentChanges)

  Operator.Build(uid, prio, eval, parents, context, contents = VWindow [])

(* Evaluator: this operator evaluates some expression and records its result *)
let makeEvaluator expr kenv (uid, prio, parents, context) =
  let operEval = fun (op, allChanges) ->
                   let env = Map.union (getOperEnvValues op) kenv
                   let result = eval env expr
                   setValueAndGetChanges op result

  let initialContents = try
                          let env = Map.union (Map.of_list [ for p in parents -> (p.Uid, p.Value) ]) kenv
                          eval env expr
                        with
                          | err -> VNull
  Operator.Build(uid, prio, operEval, parents, context, contents = initialContents)


let makeRecord (fields:list<string * Operator>) (uid, prio, parents, context) =
  let result = fields |> List.map (fun (f, _) -> (VString f, VNull)) |> Map.of_list |> ref
  assert (parents = (List.map snd fields))

  let recordOp = Operator.Build(uid, prio,
                   (fun (op, allChanges) ->
                      let recordChanges =
                        allChanges
                          |> List.mapi (fun i changes ->
                                          match changes with
                                          | x::xs -> let field, parent = fields.[i]
                                                     result := (!result).Add(VString field, parent.Value)
                                                     [RecordDiff (VString field, changes)]
                                          | _ -> [])
                          |> List.concat
                      op.Value <- VRecord !result
                      Some (op.Children, recordChanges)),
                   parents, context)

  for field, op in fields do
    result := (!result).Add(VString field, op.Value)

  recordOp


(* Converts a dynamic value into a stream of changes to the value *)
let makeToStream (uid, prio, parents, context) =
  let eval = fun ((op:Operator), inputs) ->
               let value = op.Parents.[0].Value
               let ev = VRecord (Map.of_list [VString "timestamp", VInt (Scheduler.clock().Now.TotalSeconds); VString "value", value])
               op.Value <- ev
               Some (op.Children, [Added ev])

  Operator.Build(uid, prio, eval, parents, context)


let makeRef getId (uid, prio, (parents:Operator list), context) =
  let objOper = List.hd parents
  Operator.Build(uid, prio, 
                 (fun (op, inputs) ->
                    let objValue = objOper.Value
                    let id = VRef (getId objValue)
                    setValueAndGetChanges op id),
                 parents, context)

let makeRefProjector field (uid, prio, (parents:Operator list), context) =
  let ref = parents.[0]
  
  Operator.Build(uid, prio, 
                 (fun (op, inputs) ->
                    let entityDict = match parents.[1].Value with
                                     | VDict d -> d
                                     | _ -> failwithf "The projector must be connected with the entity's dictionary."
                    if ref.Value <> VNull
                      then let refValue = match ref.Value with
                                          | VRef refValue -> refValue
                                          | _ -> failwithf "ref value is not a VRef?!"
                           let objChanges = [ for change in inputs.[1] do
                                                match change with
                                                | DictDiff (key, keyChanges) -> yield! if key = ref.Value then keyChanges else []
                                                | _ -> failwithf "Unexpected change in refProjector: %A" change ]
                           let refObject = entityDict.[refValue]
                           let fieldValue = match refObject with
                                            | VRecord r -> r.[VString field]
                                            | _ -> failwithf "The referenced object is not an object!"
                           
                           match op.Value = refObject, objChanges with
                           | true, [] -> None
                           | true, _ -> Some (op.Children, objChanges)
                           | false, [] -> setValueAndGetChanges op fieldValue
                           | false, _ -> setValueAndGetChanges op fieldValue
                      else None),
                 parents, context)

 
(* Indexes a value in a dictionary 
 * It has two parents: the dictionary and the index.
 *
 * When the dictionary changes but the index doesn't, it filters the changes to
 * the particular index in the dictionary and returns them.
 * When the index changes, it sets the new value and returns the change as a
 * [Added <new value>].
 *
 * This operator is needed (and a simple evaluator can't be used in its place)
 * because it actually pipes changes through the dictionary to the next operator,
 * while a simple evaluator would only transmit values, which might not be the
 * desired behavior (imagine that the value of the dictionary is a stream or a
 * window, or even another dictionary: an evaluator would always be transmiting
 * [Added <new value>], instead of the diffs.
 *)
let makeIndexer index (uid, prio, parents, context) =
  let eval = fun ((op:Operator), inputs) ->
               let env = getOperEnvValues op
               let key = eval env index
               let dict = match op.Parents.[0].Value with
                          | VDict dict -> dict
                          | _ -> failwithf "The parent of the indexer is not a dictionary?!"
               
               match inputs with
               | [x::xs as change; []] ->
                   // The dictionary changed: filter only the changes to our key.
                   let indexChanges = List.tryPick (function
                                                      | DictDiff (key', chg) when key = key' -> Some chg
                                                      | _ -> None)
                                                   change
                   match indexChanges with
                   | Some changes -> op.Value <- dict.[key]
                                     Some (op.Children, changes)
                   | None -> None
               | _ -> // The index changed: return [Added <new value>]
                      if Map.contains key dict
                        then op.Value <- dict.[key]
                             Some (op.Children, [Added op.Value])
                        else None         
            

  Operator.Build(uid, prio, eval, parents, context)


(* Projects a field out of a record 
 * Has one parent: the record.
 *
 * This operator expects to kinds of changes:
 * - RecordDiff (field, change): no explanation needed.
 * - Added record: See the indexer above.
 *
 * If it receives [RecordDiff's], it filters the changes to some particular
 * field and returns them.
 * If it receives an [Added record], it returns [Added record.field]
 *)
let makeProjector field (uid, prio, parents, context) =
  let fieldv = VString field
  let eval = fun ((op:Operator), inputs) ->
               let fieldChanges =
                 match List.hd inputs with
                 | [Added (VRecord record)] -> Some [Added record.[fieldv]]
                 | changes -> List.tryPick (function
                                              | RecordDiff (field', chg) when fieldv = field' -> Some chg
                                              | _ -> None)
                                           changes
               let record = match op.Parents.[0].Value with
                            | VRecord r -> r
                            | _ -> failwithf "The parent of the projector is not a record?!"
               let newValue = record.[fieldv]
               match fieldChanges with
               | Some changes -> op.Value <- newValue
                                 Some (op.Children, changes)
               | None -> None

  Operator.Build(uid, prio, eval, parents, context)
  

let makeClosure lambda closureBuilder itself (uid, prio, parents, context) =
  let eval = fun (op:Operator, inputs) -> None

  Operator.Build(uid, prio, eval, parents, context, contents = VClosureSpecial (uid, lambda, closureBuilder, context, itself))


(*
 * The first parent of the function call is the closure. The others are the
 * parameters to the call.
 *
 * The operator that represents the closure can be of two forms:
 * - a makeClosure, where the closure is a VClosureSpecial whose network may be
 *   built at any time;
 * - an evaluator whose value is a VClosure. In this case the closure is meant to
 *   be evaluated directly (no network creation shall happen).
 *)
let makeFuncCall (uid, prio, parents, context) =
  let subnetworks = ref Map.empty
  let currNetwork : ref<uid option> = ref None

  let rec headOp =
    Operator.Build(uid, prio,
      (fun (op:Operator, inputs) ->
           match op.Parents.[0].Value with
           | VClosureSpecial (closureUid, _, closureBuilder, closureContext, _) ->
               // Create the network and spread the changes to it.
               let parameterChanges = List.tl inputs
         
               if not (Map.contains closureUid !subnetworks)
                 then let roots, final = closureBuilder prio !closureContext
                      connect final resultOp id
                      subnetworks := Map.add closureUid (roots, final) (!subnetworks)
               currNetwork := Some closureUid

               let subnet, _ = (!subnetworks).[closureUid]
               for (child:Operator, parent:Operator) in (List.zip subnet (List.tl parents)) do
                 child.Value <- parent.Value
               
               // Now I must pass the parameter changes to the subnetwork
               let argsToEval : ChildData list = 
                 List.mapi (fun i arg ->
                              let link _ = parameterChanges.[i] // Ignore the argument
                              arg, 0, link)
                           subnet
                           
               // Output just the closure change: the parameters' changes are not needed
               Some (List<_> ((resultOp, 0, id)::argsToEval), List.hd inputs)
           | VClosure _ as closure ->
               currNetwork := None
               // Evaluate the expression directly.
               let env = getOperEnvValues op
               let param's = [ for parent in parents.Tail -> Id (Identifier parent.Uid) ]
               let result = eval (env.Add("$closure", closure)) (FuncCall (Id (Identifier "$closure"), param's))
               Some (List<_> ([resultOp, 0, id]), [Added result])
           | VNull -> None
           | other -> failwithf "Expecting a VClosure[Special], but the parent's value is %A" other),
      parents, context)

  and resultOp =
    Operator.Build(uid + "_results", Priority.add prio (Priority.of_list [9]),
      (fun (op:Operator, inputs) ->
        let value = match !currNetwork, inputs with
                    | None, [Added result]::_ -> result
                    | Some op, _ -> (snd (!subnetworks).[op]).Value
                    | None, other -> failwithf "Can't happen: %A" other
        setValueAndGetChanges op value), [headOp], context)
      
  { headOp with
        Children = resultOp.Children;
        Contents = resultOp.Contents;
        AllChanges = resultOp.AllChanges }

(*
 * The first parent is the evaluator for the initial expression,
 * the others are the whens.
 *)
let makeListenN (uid, prio, (parents:Operator list), context) =
  let operEval = fun ((op:Operator), inputs) ->
                   match inputs with
                   | [Added v]::rest when op.Value = VNull -> setValueAndGetChanges op v // Initializing
                   | _::listenerInputs ->
                       let parentIdx = List.tryFindIndex (fun input -> input <> []) listenerInputs
                       match parentIdx with
                       | None -> None
                       | Some idx ->
                           let closure = 
                             match op.Parents.[idx + 1].Value with
                             | VClosureSpecial _ as special ->
                                 convertClosureSpecial special
                             | VClosure _ as closure -> closure
                             | _ -> failwithf "Listener didn't return a closure!!!"
                           let env = Map.of_list ["$curr", op.Value]
                           let result = eval (env.Add("$closure", closure)) (FuncCall (Id (Identifier "$closure"), [Id (Identifier "$curr")]))
                           setValueAndGetChanges op result
                   | _ -> failwithf "Can't happen"

  Operator.Build(uid, prio, operEval, parents, context, contents = parents.[0].Value)


(* ticks *)
let makeTicks (uid, prio, parents, context) =
  let maxLimit = ref None
  let timestamp () = Scheduler.clock().Now.TotalSeconds

  let nextEvent () =
    VRecord (Map.of_list [VString "timestamp", VInt (timestamp () + 1)])
  
  let eval = fun ((op:Operator), inputs) ->
               match List.hd inputs with
               | [Expired (VInt n)] -> maxLimit := Some n // Used by the testing system to set a maximum limit.
                                       None
               | _ -> match !maxLimit with
                      | Some max when timestamp () > max -> None
                      | _ -> Scheduler.scheduleOffset 1 (List.of_seq [op, 0, id], [Added (nextEvent ())])
                             Some (op.Children, List.hd inputs)

  let op = Operator.Build(uid, prio, eval, parents, context)
  Scheduler.scheduleOffset 1 (List.of_seq [op, 0, id], [Added (nextEvent ())])
  op