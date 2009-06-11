#light

open System.Collections.Generic
open Extensions
open Ast
open Util
open Types
open Eval
open Scheduler
open Oper

(* A stream: propagates received events *)
let makeStream uid prio parents =
  let eval = fun (op, inputs) ->
               match inputs with
               | [[Added (VEvent ev)] as changes] -> Some (op.Children, changes)
               | _ -> failwith "stream: Invalid arguments!"

  Operator.Build(uid, prio, eval, parents)

(* A simple window - does not schedule expiration of events. *)
let makeSimpleWindow uid prio parents =
  let eval = fun (op, inputs) ->
               match inputs with
               | changes::_ -> Some (op.Children, changes)
               | _ -> failwith "Simple window: Invalid arguments!"

  Operator.Build(uid, prio, eval, parents)
  
(* A timed window *)
let makeWindow duration uid prio parents =
  let eval = fun (op, inputs) ->
               match inputs with
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
let makeEvalOnAdd resultHandler eventHandler uid prio parents : Operator =
    let expr = FuncCall (eventHandler, [Id (Identifier "ev")])
    let eval = fun (op, inputs) ->
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
  let eval = fun ((op:Operator), inputs) ->
               match inputs with
               | [[Added something] as changes] ->
                   if op.Value <> VNull
                     then Scheduler.scheduleOffset duration (List.of_seq op.Children, [Expired op.Value])
                   op.Value <- something
                   Some (op.Children, changes)
               | _ -> failwith "timed window: Wrong number of arguments!"

  Operator.Build(uid, prio, eval, parents)

(* Evaluator: this operator evaluates some expression and records its result *)
let makeEvaluator expr kenv uid prio parents =
  let operEval = fun (op, allChanges) ->
                   let env = Map.union (getOperEnvValues op) kenv
                   let result = eval env expr
                   setValueAndGetChanges op result

  let initialContents = try
                          let env = Map.union (Map.of_list [ for p in parents -> (p.Uid, p.Value) ]) kenv
                          eval env expr
                        with
                          | err -> VNull
  Operator.Build(uid, prio, operEval, parents, contents = initialContents)


let makeRecord (fields:list<string * Operator>) uid prio parents =
  let result = fields |> List.map (fun (f, _) -> (VString f, ref VNull)) |> Map.of_list
  assert (parents = (List.map snd fields))

  let recordOp = Operator.Build(uid, prio,
                   (fun (op, allChanges) ->
                      let recordChanges =
                        allChanges
                          |> List.mapi (fun i changes ->
                                          match changes with
                                          | x::xs -> let field, parent = fields.[i]
                                                     result.[VString field] := parent.Value
                                                     [RecordDiff (VString field, changes)]
                                          | _ -> [])
                          |> List.concat
                      Some (op.Children, recordChanges)),
                   parents, contents = VRecord result)

  for field, op in fields do
    result.[VString field] := op.Value

  recordOp


(* Converts a dynamic value into a stream of changes to the value *)
let makeToStream uid prio parents =
  let eval = fun ((op:Operator), inputs) ->
               let value = op.Parents.[0].Value
               let ev = Event (Scheduler.clock().Now, Map.of_list ["value", value])
               Some (op.Children, [Added (VEvent ev)])

  Operator.Build(uid, prio, eval, parents)


let makeRef getId uid prio (parents:Operator list) =
  let objOper = List.hd parents
  Operator.Build(uid, prio, 
                 (fun (op, inputs) ->
                    let objValue = objOper.Value
                    let id = VRef (getId objValue)
                    setValueAndGetChanges op id),
                 parents)

let makeRefProjector field uid prio (parents:Operator list) =
  let ref = parents.[0]
  let entityDict = match parents.[1].Value with
                   | VDict d -> d
                   | _ -> failwithf "The projector must be connected with the entity's dictionary."
  
  Operator.Build(uid, prio, 
                 (fun (op, inputs) ->
                    if ref.Value <> VNull
                      then let refValue = match ref.Value with
                                          | VRef refValue -> refValue
                                          | _ -> failwithf "ref value is not a VRef?!"
                           let refObject = (!entityDict).[refValue]
                           match refObject with
                           | VRecord r -> setValueAndGetChanges op !r.[VString field]
                           | _ -> failwithf "The referenced object is not an object!"
                      else None),
                 parents)

 
(* Indexes a value in a dictionary 
 * It has two parents: the dictionary and the index.
 *
 * When the dictionary changes but the index doesn't, it filters the changes to
 * the particular index in the dictionary and returns them.
 * When the index changes, it sets the new value and returns the change as a
   [Added <new value>].
 *)
let makeIndexer index uid prio (parents:Operator list) =
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
                   | Some changes -> op.Value <- (!dict).[key]
                                     Some (op.Children, changes)
                   | None -> None
               | _ -> // The index changed: return [Added <new value>]
                      if Map.contains key (!dict)
                        then op.Value <- (!dict).[key]
                             Some (op.Children, [Added op.Value])
                        else None         
            

  Operator.Build(uid, prio, eval, parents)


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
let makeProjector field uid prio (parents:Operator list) =
  let fieldv = VString field
  let eval = fun ((op:Operator), inputs) ->
               let fieldChanges =
                 match List.hd inputs with
                 | [Added (VRecord record)] -> Some [Added !record.[fieldv]]
                 | changes -> List.tryPick (function
                                              | RecordDiff (field', chg) when fieldv = field' -> Some chg
                                              | _ -> None)
                                           changes
               let record = match op.Parents.[0].Value with
                            | VRecord r -> r
                            | _ -> failwithf "The parent of the projector is not a record?!"
               let newValue = !(record.[fieldv])
               match fieldChanges with
               | Some changes -> op.Value <- newValue
                                 Some (op.Children, changes)
               | None -> None

  Operator.Build(uid, prio, eval, parents)
  
// Doesn't do anything.
// TODO: Remove this operator
let makeClosure closureBuilder uid prio parents =
  let eval = fun (op:Operator, inputs) -> None
  Operator.Build(uid, prio, eval, parents, contents = VClosureSpecial (uid, closureBuilder))
  
let makeFuncCall uid prio parents =
  let subnetworks = ref Map.empty
  let currNetwork : ref<uid option> = ref None

  let rec headOp =
    Operator.Build(uid, prio,
      (fun (op:Operator, inputs) ->
         printfn "I am %s and these are my inputs %A"  uid inputs
         let env = getOperEnv op
         let closureUid, closureBuilder =
           match op.Parents.[0].Value with
           | VClosureSpecial (uid, builder) -> uid, builder
           | other -> failwithf "Expecting a VClosureSpecial, but the parent's value is %A" other
         let parameterChanges = List.tl inputs
         
         if not (Map.contains closureUid !subnetworks)
           then let roots, final = closureBuilder prio env
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
         Some (List<_> ((resultOp, 0, id)::argsToEval), List.hd inputs)),
      parents)

  and resultOp =
    Operator.Build(uid + "_results", Priority.add prio (Priority.of_list [9]),
      (fun (op:Operator, inputs) ->
        let resultOp = (snd (!subnetworks).[(!currNetwork).Value])
        printfn "%s - vou por o meu valor a %A" uid resultOp.Value
        setValueAndGetChanges op resultOp.Value), [headOp])
      
  { headOp with
        Children = resultOp.Children;
        Contents = resultOp.Contents;
        AllChanges = resultOp.AllChanges }