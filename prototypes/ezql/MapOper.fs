#light

open System.Collections.Generic
open Types
open Oper

let makeGroupby field groupBuilder uid prio parents =
    let substreams = ref Map.empty
    let results = new Dictionary<value, value>()

    let rec buildSubGroup key env =
      let initial, final = groupBuilder prio env
      substreams := (!substreams).Add(key, initial)
      connect groupOp initial id
      // Connects the resulting node to the dictionary operator.
      // The diff is converted into a DictDiff before being passed to the child.
      connect final dictOp (fun changes -> [DictDiff (key, changes)])

    and groupOp = Operator.Build(uid, prio,
                    (fun op changes ->
                       match changes with
                       | [Added (VEvent ev)]::_ -> let key = ev.[field]
                                                   let env = Map.of_list [ for p in op.Parents do
                                                                             if p <> op.Parents.[0]
                                                                               then yield (p.Uid, p) ]

                                                   if not (Map.mem key !substreams)
                                                     then buildSubGroup key env

                                                   let group = (!substreams).[key]
                                                   Some (List<_>((dictOp, 0, fun changes -> [DictDiff (key, changes)])
                                                                   ::[group, 0, id]), changes.Head)
                       | []::_ -> None
                       | other -> failwithf "Invalid arguments for groupby! %A" other),
                    parents)


    and dictOp = Operator.Build(uid + "_dict", prio + 0.9,
                   (fun op changes ->
                      match changes with
                      | parentChanges::groupChanges -> 
                          List.iteri (fun i chg ->
                                        match chg with
                                        | [] -> ()
                                        | [DictDiff (key, _)] -> results.[key] <- op.Parents.[i + 1].Value
                                        | _ -> failwithf "Dictionary expects all diffs to be of type DictDiff but received %A" chg)
                                     groupChanges
                          let allChanges =
                            (List.map_concat (fun chg ->
                                                match chg with
                                                | DictDiff (key, _) when not (results.ContainsKey(key)) ->
                                                    // If this happens, the groupby result must not depend on
                                                    // the group's substream. Hence, it depends only on values
                                                    // in the outer scope. Hence, every subgroup has the same value!
                                                    results.[key] <- op.Parents.[1].Value
                                                    [DictDiff (key, [Added results.[key]])]
                                                | _ -> [])
                                            parentChanges) @ (List.concat groupChanges)
                          match allChanges with
                          | [] -> None
                          | _ -> Some (op.Children, allChanges)
                      | _ -> failwith "Empty changes?"),
                   [groupOp], contents = VDict results)


    { groupOp with
        Children = dictOp.Children;
        Contents = dictOp.Contents }


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
    let results = Dictionary<value, value>()

    let predStartOp key = fst (!predicates).[key]
    let predResultOp key = snd (!predicates).[key]

    let rec buildPredCircuit key env =
      let initial, final = predicateBuilder prio env
      predicates := (!predicates).Add(key, (initial, final))

      // Connects the resulting node to the dictionary operator.
      // The diff is converted into a DictDiff before being passed to the child.
      connect final dictOp (fun changes -> [DictDiff (key, changes)])

    and whereOp = Operator.Build(uid, prio,
                    (fun op changes ->
                      let env = Map.of_list [ for p in op.Parents do
                                                if p <> op.Parents.[0]
                                                  then yield (p.Uid, p) ]
                      let parentDiff = changes.Head
                      let predsToEval = List.map_concat
                                          (function
                                             | DictDiff (key, diff) ->
                                                 if not (Map.mem key !predicates) then buildPredCircuit key env

                                                 // The link between the where and the group's circuit ignores everything
                                                 // that is not related to that group.
                                                 let link = (fun changes -> [ for diff in changes do
                                                                                match diff with
                                                                                | DictDiff (key', v) when key = key' -> yield! v
                                                                                | _ -> () ])
                                                 [predStartOp key, 0, link]
                                             | RemovedKey (key) -> []
                                             | chg -> failwithf "Map/where received invalid changes: %A" chg)
                                          parentDiff
                      Some (List<_> ((dictOp, 0, id)::predsToEval), parentDiff)),
                      parents)

    and dictOp = Operator.Build(uid + "_dict", prio + 0.9,
                   (fun op changes ->
                      // changes.Head contains the dictionary changes passed to the where
                      // changes.Tail contains the changes in the inner predicates
                      let parentChanges, predChanges = changes.Head, changes.Tail
                      let parentDict = match whereOp.Parents.[0].Value with
                                       | VDict d -> d
                                       | _ -> failwith "The parent of this where is not a dictionary!"

                      (* First, take care of the changes reported by the parent dictionary *)
                      let keys1, changes1 =
                        List.unzip [ for change in parentChanges do
                                       match change with
                                       | DictDiff (key, v) ->
                                           match (predResultOp key).Value, results.ContainsKey(key) with
                                           | VBool true, _ -> results.[key] <- parentDict.[key]
                                                              yield key, change
                                           | VBool false, true -> results.Remove(key) |> ignore
                                                                  yield key, RemovedKey key
                                           | _ -> ()
                                       | RemovedKey key ->
                                           if results.ContainsKey(key)
                                             then results.Remove(key) |> ignore
                                                  yield key, change
                                             else ()
                                       | _ -> failwithf "Invalid change received in dict/where: %A" change ]

                      let keys1' = Set.of_list keys1

                      (* Now we still have to take care of predicate changes for the keys
                         which have not been handled already *)
                      let changes2 =
                        [ for change in predChanges do
                            match change with
                            | [] -> ()
                            | [DictDiff (key, [(Added (VBool v))])] when not (Set.mem key keys1') ->
                                if v then results.[key] <- parentDict.[key]
                                          yield DictDiff (key, [Added results.[key]])
                                     else if results.ContainsKey(key)
                                             then results.Remove(key) |> ignore
                                                  yield RemovedKey key
                            | [DictDiff (key, _)] when (Set.mem key keys1') -> ()
                            | _ -> failwithf "The predicate was supposed to return a boolean, but instead returned %A" change ]

                      let allChanges = changes1 @ changes2
                      match allChanges with
                      | [] -> None
                      | _ -> Some (op.Children, allChanges)),
                   [], contents = VDict results)

    { whereOp with
        Children = dictOp.Children;
        Contents = dictOp.Contents }


let makeDictSelect projectorBuilder uid prio parents =
    let projectors = ref Map.empty
    let results = Dictionary<value, value>()
    
    let projStartOp key = fst (!projectors).[key]
    let projResultOp key = snd (!projectors).[key]


    let rec buildProjCircuit key env =
      let initial, final = projectorBuilder prio env
      projectors := (!projectors).Add(key, (initial, final))

      // Connects the resulting node to the dictionary operator.
      // The diff is converted into a DictDiff before being passed to the child.
      connect final dictOp (fun changes -> [DictDiff (key, changes)])

    and selectOp = Operator.Build(uid, prio,
                     (fun op changes ->
                        let env = Map.of_list [ for p in op.Parents do
                                                  if p <> op.Parents.[0]
                                                    then yield (p.Uid, p) ]
                        let parentChanges = changes.Head
                        let projsToEval = List.map_concat (function
                                                             | DictDiff (key, diff) ->
                                                                 if not (Map.mem key !projectors) then buildProjCircuit key env

                                                                 // The link between the where and the group's circuit ignores everything
                                                                 // that is not related to that group.
                                                                 let link = (fun changes -> [ for diff in changes do
                                                                                                match diff with
                                                                                                | DictDiff (key', v) when key = key' -> yield! v
                                                                                                | _ -> () ])
                                                                 [projStartOp key, 0, link]
                                                             | RemovedKey key -> []
                                                             | _ -> failwith "Map/select received invalid changes")
                                                          parentChanges
                        Some (List<_> ((dictOp, 0, id)::projsToEval), parentChanges)),
                     parents)

    and dictOp = Operator.Build(uid + "_dict", prio + 0.9,
                   (fun op changes ->
                      // changes.Head contains the dictionary changes passed to the select
                      // changes.Tail contains the changes in the inner predicates
                      let parentChanges, projChanges = changes.Head, changes.Tail
                                                       
                      let keys1, changes1 =
                        List.unzip [ for chg in parentChanges do
                                       match chg with
                                       | DictDiff (key, _) when not (results.ContainsKey(key)) ->
                                           results.[key] <- (projResultOp key).Value
                                           yield key, DictDiff (key, [Added results.[key]])
                                       | RemovedKey key -> assert results.Remove(key)
                                                           yield key, chg
                                       | _ -> () ]
                      let keys1' = Set.of_list keys1
                      
                      let changes2 =
                        [ for change in projChanges do
                            match change with
                            | [] -> ()
                            | [DictDiff (key, _)] -> 
                                if not (Set.mem key keys1')
                                  then results.[key] <- (projResultOp key).Value
                                       yield DictDiff (key, [Added results.[key]])
                            | _ -> failwithf "The predicate was supposed to return a boolean, but instead returned %A" change ]
                 
                      let allChanges = changes1 @ changes2
                      match allChanges with
                      | [] -> None
                      | _ -> Some (op.Children, allChanges)),                      
                   [selectOp], contents = VDict results)

    { selectOp with
        Children = dictOp.Children;
        Contents = dictOp.Contents }
