﻿#light

open System.Collections.Generic
open Util
open Types
open Oper

type SubCircuitMap = Map<value, Operator * Operator>

let subGroupStartOp key (subgroups:SubCircuitMap ref) = fst (!subgroups).[key]
let subGroupResultOp key (subgroups:SubCircuitMap ref) = snd (!subgroups).[key]

let spreadUnlessEmpty op changes =
  match changes with
  | [] -> None
  | _ -> Some (op.Children, changes)


(* Used by the dict.where() and dict.select() methods to give an operator to
   the parameter of the predicate or projector.
   The value of this operators is manually set by the methods that use it.
 *)
let makeInitialOp uid prio parents =
  let eval = fun op inputs -> Some (op.Children, List.hd inputs)

  Operator.Build(uid, prio, eval, parents)


(*
 * Creates the initial operator used by where and select
 *)     
let makeHeadOp dictOp (subgroups:SubCircuitMap ref) groupBuilder uid prio (parents:Operator list) =
  let rec buildSubGroup key env headOp =
    let initial, final = groupBuilder prio env
    subgroups := (!subgroups).Add(key, (initial, final))

    // Connects the resulting node to the dictionary operator.
    // The diff is converted into a DictDiff before being passed to the child.
    connect final dictOp (fun changes -> [DictDiff (key, changes)])
       
  let makeInit () =
    match (parents.Head).Value with
    | VDict dict -> let initialChanges = ref (Seq.fold (fun acc (x:KeyValuePair<value, value>) ->
                                                         (DictDiff (x.Key, [Added x.Value]))::acc)
                                                       [] (!dict))
                    fun changes -> let changes' = changes @ !initialChanges
                                   initialChanges := []
                                   changes'
    | _ -> id
    
    
  let initOnce = makeInit ()

  Operator.Build(uid, prio,
    (fun op changes ->
       let parentDict =
         match (parents.Head).Value with
         | VDict dict -> dict
         | _ -> failwithf "Can't happen"   
       let env = getOperEnv op
       //printfn "%s_head: %A" op.Uid changes.Head
       let parentChanges = initOnce changes.Head
       let predsToEval = 
         [ for chg in parentChanges do
             match chg with
             | DictDiff (key, diff) ->
                 if not (Map.contains key !subgroups) then buildSubGroup key env op

                 // The link between this operator and the group's circuit ignores everything
                 // that is not related to that group.
                 let link = (fun changes -> [ for diff in changes do
                                                match diff with
                                                | DictDiff (key', v) when key = key' -> yield! v
                                                | _ -> () ])
                 let keyOp = subGroupStartOp key subgroups
                 // Manually set the value of the subgroup's initial operator
                 keyOp.Value <- (!parentDict).[key]
                 keyOp.AllChanges := [Added keyOp.Value]
                 yield subGroupStartOp key subgroups, 0, link
             | RemovedKey (key) -> ()
             | Added (VDict dict) when (!dict).IsEmpty -> () // Received on parent initialization
             | chg -> printfn "%A received invalid changes: %A" uid chg ]

       Some (List<_> ((dictOp, 0, id)::predsToEval), parentChanges)), parents)

     
let makeGroupby field groupBuilder uid prio parents =
    let substreams = ref Map.empty
    let results = ref Map.empty

    // Filter only the changes related to the given key
    let filterKey k =
      List.filter (fun change ->
                     match change with
                     | Added (VEvent ev) | Expired (VEvent ev) -> ev.[field] = k
                     | _ -> false)

    // Collect changes by key returning a list of DictDiff's
    let collectByKey =
      List.fold (fun acc change -> 
                   match change with
                   | Added (VEvent ev) | Expired (VEvent ev) ->                                 
                       let key = ev.[field]
                       let v' = if Map.contains key acc then change :: acc.[key] else [change]
                       Map.add key v' acc
                   | _ -> failwith "Invalid changes")
                Map.empty
        >> Map.to_list
        >> List.map DictDiff

    let rec buildSubGroup key env =
      let initial, final = groupBuilder prio env
      substreams := (!substreams).Add(key, initial)
      connect groupOp initial id
      // Connects the resulting node to the dictionary operator.
      // The diff is converted into a DictDiff before being passed to the child.
      connect final dictOp (fun changes -> [DictDiff (key, changes)])

    and groupOp = Operator.Build(uid, prio,
                    (fun op changes ->
                       let env = getOperEnv op
                       let children = 
                         [ for change in changes.Head ->
                             match change with
                             | Added (VEvent ev) | Expired (VEvent ev) ->
                                 let key = ev.[field]

                                 if not (Map.contains key !substreams)
                                   then buildSubGroup key env

                                 (!substreams).[key], key
                             | _ -> failwithf "Invalid arguments for groupby! %A" change ]
                       let childrenNoDup = children |> Set.of_list |> Set.to_list
                       match children with
                       | [] -> None
                       | _ -> let childrenData = List.map (fun (g, k) -> g, 0, filterKey k) childrenNoDup
                              Some (List<_>((dictOp, 0, collectByKey)::childrenData), changes.Head)),
                    parents)

    and dictOp = Operator.Build(uid + "_dict", Priority.add prio (Priority.of_list [9]),
                   (fun op changes ->
                      match changes with
                      | parentChanges::groupChanges -> 
                          let groupChanges' =
                            List.mapi (fun i chg ->
                                         match chg with
                                         | [DictDiff (key, _)] ->
                                             let added = not ((!results).ContainsKey(key))
                                             let value = op.Parents.[i + 1].Value
                                             results := (!results).Add(key, value)
                                             // If the key was just added, return [Added <value>]. Otherwise just return the original change.
                                             if added then [DictDiff (key, [Added value])] else chg
                                         | [] -> []
                                         | _ -> failwithf "Dictionary expects all diffs to be of type DictDiff but received %A" chg)
                                      groupChanges

                          let allChanges =
                            (List.collect (fun chg ->
                                             match chg with
                                             | DictDiff (key, _) when not (Map.contains key !results) ->
                                                 // If this happens, the groupby result must not depend on
                                                 // the group's substream. Hence, it depends only on values
                                                 // in the outer scope. Hence, every subgroup has the same value!
                                                 results := (!results).Add(key, op.Parents.[1].Value)
                                                 [DictDiff (key, [Added (!results).[key]])]
                                             | _ -> [])
                                        parentChanges) @ (List.concat groupChanges')
                          spreadUnlessEmpty op allChanges
                      | _ -> failwith "Empty changes?"),
                   [groupOp], contents = VDict results)

    { groupOp with
        Children = dictOp.Children;
        Contents = dictOp.Contents;
        AllChanges = dictOp.AllChanges }


(*
 * Structure:
 *
 * - whereOp receives the changes from the parent dictionary and propagates
 *   them to the right predicate circuits;
 * - whereOp also sends these changes to dictOp;
 * - dictOp receives the changes from whereOp and the changes from each
 *   predicate and updates the results dictionary accordingly.
 *)
let makeDictWhere predicateBuilder uid prio parents =
    // Contains the initial and final operator for each key's predicate
    let predicates = ref Map.empty
    let results = ref Map.empty
 
    let dictOp = Operator.Build(uid + "_dict", Priority.add prio (Priority.of_list [9]),
                   (fun op changes ->
                      // changes.Head contains the dictionary changes passed to the where
                      // changes.Tail contains the changes in the inner predicates
                      let parentChanges, predChanges = changes.Head, changes.Tail
                      let whereOp = op.Parents.[0]
                      let parentDict = match whereOp.Parents.[0].Value with
                                       | VDict d -> !d
                                       | _ -> failwith "The parent of this where is not a dictionary!"

                      (* First, take care of the changes reported by the parent dictionary *)
                      let keys1, changes1 =
                        List.unzip [ for change in parentChanges do
                                       match change with
                                       | DictDiff (key, v) ->
                                           match (subGroupResultOp key predicates).Value, Map.contains key !results with
                                           // If we already have the key, modify it and yield the diff.
                                           | VBool true, true -> results := (!results).Add(key, parentDict.[key])
                                                                 yield key, change
                                           // If we don't have the key, add it and yield an [Added ...] diff.
                                           | VBool true, false -> results := (!results).Add(key, parentDict.[key])
                                                                  yield key, DictDiff (key, [Added parentDict.[key]])
                                           // Remove the key if we contain it.
                                           | VBool false, true -> results := (!results).Remove(key)
                                                                  yield key, RemovedKey key
                                           | _ -> ()
                                       | RemovedKey key -> // Remove the key if we contain it.
                                           if Map.contains key (!results)
                                             then results := (!results).Remove(key)
                                                  yield key, change
                                             else ()
                                       | Added (VDict dict) when (!dict).IsEmpty -> () // Received on parent initialization
                                       | _ -> failwithf "Invalid change received in dict/where: %A" change ]
                      let keys1' = Set.of_list keys1

                      (* Now we still have to take care of predicate changes for the keys
                         which have not been handled already *)
                      let changes2 =
                        [ for change in predChanges do
                            match change with
                            | [] -> ()
                            | [DictDiff (key, [(Added (VBool v))])] when not (Set.contains key keys1') ->
                                if v then results := (!results).Add(key, parentDict.[key])
                                          yield DictDiff (key, [Added (!results).[key]])
                                     else if Map.contains key !results
                                             then results := (!results).Remove(key)
                                                  yield RemovedKey key
                            | [DictDiff (key, _)] when (Set.contains key keys1') -> ()
                            | _ -> failwithf "The predicate was supposed to return a boolean, but instead returned %A" change ]

                      spreadUnlessEmpty op (changes1 @ changes2)),
                   [], contents = VDict results)
     
    let whereOp = makeHeadOp dictOp predicates predicateBuilder uid prio parents                  
    connect whereOp dictOp id

    { whereOp with
        Children   = dictOp.Children;
        Contents   = dictOp.Contents;
        AllChanges = dictOp.AllChanges }


let makeDictSelect projectorBuilder uid prio parents =
    let projectors = ref Map.empty
    let results = ref Map.empty

    let dictOp = Operator.Build(uid + "_dict", Priority.add prio (Priority.of_list [9]),
                   (fun op changes ->
                      //printfn "Antes %s: Value = %O Changes = %A" op.Uid op.Value changes
                      //printfn "Parent = %s" op.Parents.[1].Uid
                      // changes.Head contains the dictionary changes passed to the select
                      // changes.Tail contains the changes in the inner predicates
                      let parentChanges, projChanges = changes.Head, changes.Tail
                                                       
                      let keys1, changes1 =
                        List.unzip [ for chg in parentChanges do
                                       match chg with
                                       | DictDiff (key, _) when not (Map.contains key !results) ->
                                           results := (!results).Add(key, (subGroupResultOp key projectors).Value)
                                           yield key, DictDiff (key, [Added (!results).[key]])
                                       | RemovedKey key -> results := (!results).Remove(key)
                                                           yield key, chg
                                       | _ -> () ]
                      let keys1' = Set.of_list keys1
                      let changes2 =
                        [ for change in projChanges do
                            match change with
                            | [] -> ()
                            | [DictDiff (key, _)] -> 
                                if (not (Set.contains key keys1')) && (Map.contains key (!results))
                                  then results := (!results).Add(key, (subGroupResultOp key projectors).Value)
                                       yield DictDiff (key, [Added (!results).[key]])
                            | _ -> failwithf "The predicate was supposed to return a boolean, but instead returned %A" change ]
                      //printfn "Depois %s: Value = %O" op.Uid op.Value
                      spreadUnlessEmpty op (changes1 @ changes2)),                      
                   [], contents = VDict results)
    
    let selectOp = makeHeadOp dictOp projectors projectorBuilder uid prio parents    
    connect selectOp dictOp id

    { selectOp with
        Children   = dictOp.Children;
        Contents   = dictOp.Contents;
        AllChanges = dictOp.AllChanges }
