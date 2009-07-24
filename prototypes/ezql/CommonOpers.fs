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
               let change = List.tryFind (function
                                            | Added (VRecord _) as change -> true
                                            | _ -> false)
                                         (List.hd inputs)
                                      
               match change with
               | Some (Added (VRecord _ as ev) as change') ->
                   op.Value <- ev
                   SpreadChildren [change']
               | _ -> Nothing

  Operator.Build(uid, prio, eval, parents, context)

(* A simple window - does not schedule expiration of events. *)
let makeSimpleWindow (uid, prio, parents, context) =
  let contents = ref []

  let eval = fun (op:Operator, inputs) ->
               let parentChanges = List.hd inputs
               for change in parentChanges do
                 match change with
                 | Added ev -> contents := (!contents) @ [ev]
                 | Expired ev -> assert ((!contents).Head = ev)
                                 contents := (!contents).Tail
                 | _ -> failwithf "Invalid changes to window: %A" parentChanges
               op.Value <- VWindow !contents
               SpreadChildren parentChanges

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
               SpreadChildren parentChanges

  Operator.Build(uid, prio, eval, parents, context)


(* Where: propagates events that pass a given predicate *)
let makeWhere predicate isWindow (uid, prio, parents:Operator list, context) =
  let contents = ref []

  let setOrAddValue (op:Operator) value =
    if isWindow
      then contents := (!contents) @ [value]
           op.Value <- VWindow !contents
      else op.Value <- value

  let expireIfWindow (op:Operator) =
    if isWindow
      then contents := (!contents).Tail
           op.Value <- VWindow !contents
      
  let expr = FuncCall (predicate, [Id (Identifier "ev")])
  let applyPredicate op ev =
    let env = getOperEnvValues op
    let env' = Map.add "ev" ev env
    match eval env' expr with
    | VBool bool -> bool
    | other -> failwithf "where's predicate should return a boolean but returned %A" other

  let rec opTop = Operator.Build(uid, prio,
                    (fun (op, inputs) -> 
                       match List.hd inputs with
                       | [] -> Nothing
                       | ev -> Delay (List<_>([opBottom, 0, id]), ev)),
                    parents, context)

  and opBottom = Operator.Build(uid + "_bottom", Priority.add prio (Priority.of_list [9]),
                   (fun (op, inputs) ->
                      let output = [ for change in List.hd inputs do
                                       match change with
                                       | Added (VWindow contents') -> 
                                           assert isWindow
                                           contents := List.filter (applyPredicate opTop) contents'
                                           op.Value <- VWindow !contents
                                           yield Added op.Value
                                       | Added ev -> if applyPredicate opTop ev
                                                       then setOrAddValue op ev
                                                            yield Added ev
                                       | Expired ev -> if applyPredicate opTop ev
                                                         then expireIfWindow op
                                                              yield Expired ev
                                       | _ -> failwithf "where received an unexpected change: %A" change ]
                                      
                      match output with
                      | [] -> Nothing
                      | _ -> SpreadChildren output),
                   [opTop], context) 
                              
  { opTop with
        Children = opBottom.Children;
        Contents = opBottom.Contents;
        AllChanges = opBottom.AllChanges }  
        

(* Select: projects an event *)
let makeSelect handler isWindow (uid, prio, parents:Operator list, context) =
  let contents = ref []

  let setOrAddValue (op:Operator) value =
    if isWindow
      then contents := (!contents) @ [value]
           op.Value <- VWindow !contents
      else op.Value <- value

  let expireIfWindow (op:Operator) =
    if isWindow
      then contents := (!contents).Tail
           op.Value <- VWindow !contents

  let expr = FuncCall (handler, [Id (Identifier "ev")])
  let project env ev =
    let timestamp = eventTimestamp ev
    let env' = Map.add "ev" ev env
    let result = eval env' expr    
    match result with
    | VRecord fields -> VRecord (Map.add (VString "timestamp") timestamp fields)
    | _ -> failwithf "select should return a record"
 
  let rec opTop = Operator.Build(uid, prio,
                    (fun (op, inputs) -> 
                       match List.hd inputs with
                       | [] -> Nothing
                       | ev -> Delay (List<_>([opBottom, 0, id]), ev)),
                    parents, context)

  and opBottom = Operator.Build(uid + "_bottom", Priority.add prio (Priority.of_list [9]),
                   (fun (op, inputs) ->
                      let env = getOperEnvValues opTop
                      let output = [ for change in List.hd inputs do
                                        match change with
                                        | Added (VWindow contents') -> 
                                           assert isWindow
                                           contents := List.map (project env) contents'
                                           op.Value <- VWindow !contents
                                           yield Added op.Value
                                        | Added ev -> let ev' = project env ev
                                                      setOrAddValue op ev'
                                                      yield Added ev'
                                        | Expired ev -> let ev' = project env ev
                                                        expireIfWindow op
                                                        yield Expired ev'
                                        | _ -> failwithf "select received an unexpected change: %A" change ]
                      match output with
                      | [] -> Nothing
                      | _ -> SpreadChildren output),
                   [opTop], context) 
                              
  { opTop with
        Children = opBottom.Children;
        Contents = opBottom.Contents;
        AllChanges = opBottom.AllChanges }  
  

(* When is composed of two operators: the first receives inputs and postpones their
   evaluation, which will be done by the second operator, in a second eval stage. *)
let makeWhen eventHandler (uid, prio, parents, context) =
  let expr = FuncCall (eventHandler, [Id (Identifier "ev")])
  
  let rec opTop = Operator.Build(uid, prio,
                    (fun (op, inputs) -> 
                       match List.hd inputs with
                       | [] -> Nothing
                       | ev -> Delay (List<_>([opBottom, 0, id]), ev)),
                    parents, context)
                  
  and opBottom = Operator.Build(uid + "_bottom", Priority.add prio (Priority.of_list [9]),
                   (fun (op, inputs) ->
                      let env = getOperEnvValues opTop
                      
                      // eval all received events and collect the result of the last.
                      let result = List.fold (fun acc change ->
                                                match change with
                                                | Added ev ->
                                                    let env' = Map.add "ev" ev env
                                                    eval env' expr
                                                | _ -> acc)
                                             VNull (List.hd inputs)
                      
                      setValueAndGetChanges op result),
                   [opTop], context) 
                              
  { opTop with
        Children = opBottom.Children;
        Contents = opBottom.Contents;
        AllChanges = opBottom.AllChanges }


(* A timed window for dynamic values *)
let makeDynValWindow duration (uid, prio, parents, context) =
  let contents = ref [VNull]

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
               SpreadChildren parentChanges

  Operator.Build(uid, prio, eval, parents, context, contents = VWindow !contents)


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
                      if op.Value = VNull
                        then for field, parent in fields do
                               result := (!result).Add(VString field, parent.Value)
                             op.Value <- VRecord !result
                             SpreadChildren [Added op.Value]
                        else let recordChanges =
                               allChanges
                                 |> List.mapi (fun i changes ->
                                                 match changes with
                                                 | x::xs -> let field, parent = fields.[i]
                                                            result := (!result).Add(VString field, incorporateChanges changes (!result).[VString field])
                                                            //result := (!result).Add(VString field, parent.Value)
                                                            [RecordDiff (VString field, changes)]
                                                 | _ -> [])
                                 |> List.concat
                             op.Value <- VRecord !result
                             SpreadChildren recordChanges),
                   parents, context)

//  for field, op in fields do
//    result := (!result).Add(VString field, op.Value)

  recordOp


(* Converts a dynamic value into a stream of changes to the value *)
let makeToStream (uid, prio, parents, context) =
  let eval = fun ((op:Operator), inputs) ->
               // Ignore all HidKeyDiffs for keys that were already hidden before
               let any = List.exists (fun changes ->
                                        List.exists (function
                                                       | HidKeyDiff (_, false, _) -> false
                                                       | _ -> true) 
                                                    changes)
                                     inputs
                                     
               if any
                 then let value = op.Parents.[0].Value
                      let ev = VRecord (Map.of_list [VString "timestamp", VInt (Scheduler.now ()); VString "value", value])
                      op.Value <- ev
                      SpreadChildren [Added ev]
                 else Nothing

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
                                                | VisKeyDiff (key, keyChanges) -> yield! if key = ref.Value then keyChanges else []
                                                | _ -> failwithf "Unexpected change in refProjector: %A" change ]
                           let refObject = entityDict.[refValue]
                           let fieldValue = match refObject with
                                            | VRecord r -> r.[VString field]
                                            | _ -> failwithf "The referenced object is not an object!"
                           
                           match op.Value = refObject, objChanges with
                           | true, [] -> Nothing
                           | true, _ -> SpreadChildren objChanges
                           | false, [] -> setValueAndGetChanges op fieldValue
                           | false, _ -> setValueAndGetChanges op fieldValue
                      else Nothing),
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
               | [x::xs as changes; []] ->
                   let keyChanges =
                     [ for change in changes do
                         match change with
                         | VisKeyDiff (key', chg) when key = key' ->
                             if op.Value = VNull
                               then yield! [Added dict.[key]]
                               else yield! chg
                         | HidKeyDiff (key', true, chg) when key = key' -> yield! [Added VNull]
                         | _ -> () ]
                   
                   match keyChanges with
                   | [] -> Nothing
                   | [Added VNull] -> op.Value <- VNull
                                      SpreadChildren keyChanges
                   | _ -> op.Value <- dict.[key]
                          SpreadChildren keyChanges
               | _ -> // The index changed: return [Added <new value>]
                      if Map.contains key dict
                        then op.Value <- dict.[key]
                             SpreadChildren [Added op.Value]
                        else if op.Value <> VNull
                               then op.Value <- VNull
                                    SpreadChildren [Added VNull]
                               else Nothing         
            

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
                 | [Added VNull] -> Some [Added VNull]
                 | changes -> List.tryPick (function
                                              | RecordDiff (field', chg) when fieldv = field' -> Some chg
                                              | _ -> None)
                                           changes

               match fieldChanges with
               | Some changes -> op.Value <- incorporateChanges changes op.Value
                                 SpreadChildren changes
               | _ -> Nothing

  Operator.Build(uid, prio, eval, parents, context)
  

let makeClosure lambda closureBuilder itself (uid, prio, parents, context) =
  let eval = fun (op:Operator, inputs) -> Nothing

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
               SpreadTo (List<_> ((resultOp, 0, id)::argsToEval), List.hd inputs)
           | VClosure _ as closure ->
               currNetwork := None
               // Evaluate the expression directly.
               let env = getOperEnvValues op
               let param's = [ for parent in parents.Tail -> Id (Identifier parent.Uid) ]
               let result = eval (env.Add("$closure", closure)) (FuncCall (Id (Identifier "$closure"), param's))
               SpreadTo (List<_> ([resultOp, 0, id]), [Added result])
           | VNull -> Nothing
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
                       | None -> Nothing
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
  let timestamp () = Scheduler.now()

  let nextEvent () =
    VRecord (Map.of_list [VString "timestamp", VInt (timestamp () + 1)])
  
  let eval = fun ((op:Operator), inputs) ->
               match List.hd inputs with
               | [Expired (VInt n)] -> maxLimit := Some n // Used by the testing system to set a maximum limit.
                                       Nothing
               | _ -> match !maxLimit with
                      | Some max when timestamp () > max -> Nothing
                      | _ -> Scheduler.scheduleOffset 1 (List.of_seq [op, 0, id], [Added (nextEvent ())])
                             SpreadChildren (List.hd inputs)

  let op = Operator.Build(uid, prio, eval, parents, context)
  Scheduler.scheduleOffset 1 (List.of_seq [op, 0, id], [Added (nextEvent ())])
  op


(* merge *)
let makeMerge joinField allFields (uid, prio, parents, context) =
 
  let tryPickEvent changes =
    List.tryPick (fun chg -> match chg with
                             | Added (VRecord fields) -> Some fields
                             | _ -> None)
                 changes
 
  let completeWithNull fields =
    Set.fold (fun acc f -> if Map.contains (VString f) acc
                             then acc
                             else acc.Add(VString f, VNull))
             fields allFields
 
  let eval = fun ((op:Operator), inputs:changes list) ->
               let ev1 = tryPickEvent inputs.[0]
               let ev2 = tryPickEvent inputs.[1]
               
               let changes = match ev1, ev2 with
                             | Some ev1', None -> [Added (VRecord (completeWithNull ev1'))]
                             | None, Some ev2' -> [Added (VRecord (completeWithNull ev2'))]
                             | Some ev1', Some ev2' when ev1'.[joinField] = ev2'.[joinField] -> [Added (VRecord (Map.merge (fun a b -> a) ev1' ev2'))]
                             | Some ev1', Some ev2' -> [Added (VRecord (completeWithNull ev1')); Added (VRecord (completeWithNull ev2'))]
                             | _ -> failwithf "Can't happen"
               
               SpreadChildren changes

  Operator.Build(uid, prio, eval, parents, context)


(* sortBy *)
let makeSortBy getField (uid, prio, parents, context) =
  let contents = ref []
  let eval = fun ((op:Operator), inputs) ->
               let output = [ for change in List.hd inputs do
                                match change with
                                | Added (VWindow contents') -> 
                                   contents := List.sortBy getField contents'
                                   op.Value <- VWindow !contents
                                   yield Added op.Value
                                | Added ev -> contents := List.sortBy getField (ev::!contents)
                                              op.Value <- VWindow !contents
                                              yield Added ev
                                | Expired ev -> contents := List.removeFirst ev !contents
                                                op.Value <- VWindow !contents
                                                yield Expired ev
                                | _ -> failwithf "select received an unexpected change: %A" change ]
               match output with
               | [] -> Nothing
               | _ -> SpreadChildren output
 
  Operator.Build(uid, prio, eval, parents, context)
  