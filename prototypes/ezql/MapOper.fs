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
                                                   Some (List<_>([group, 0, id]), changes.Head)
                       | []::_ -> None
                       | other -> failwithf "Invalid arguments for groupby! %A" other),
                    parents)


    and dictOp = Operator.Build(uid + "_dict", prio + 0.9,
                   (fun op changes ->
                      List.iteri (fun i chg ->
                                    match chg with
                                    | [] -> ()
                                    | [DictDiff (key, _)] -> results.[key] <- op.Parents.[i].Value // TODO remove values that didn't change
                                    | _ -> failwithf "Dictionary expects all diffs to be of type DictDiff but received %A" chg)
                                  changes
                      Some (op.Children, List.map_concat id changes)),
                   [], contents = VDict results)


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
    let predicates = ref Map.empty
    let results = Dictionary<value, value>()

    let rec buildPredCircuit key env =
      let initial, final = predicateBuilder prio env
      predicates := (!predicates).Add(key, initial)

      // Connects the resulting node to the dictionary operator.
      // The diff is converted into a DictDiff before being passed to the child.
      connect final dictOp (fun changes -> [DictDiff (key, changes)])

    and whereOp = Operator.Build(uid, prio,
                    (fun op changes ->
                      let env = Map.of_list [ for p in op.Parents do
                                                if p <> op.Parents.[0]
                                                  then yield (p.Uid, p) ]

                      let parentDiff = changes.Head
                      let predsToEval = List.map (function
                                                  | DictDiff (key, diff) ->
                                                      if not (Map.mem key !predicates) then buildPredCircuit key env

                                                      // The link between the where and the group's circuit ignores everything
                                                      // that is not related to that group.
                                                      let link = (fun changes -> [ for diff in changes do
                                                                                     match diff with
                                                                                     | DictDiff (key', v) when key = key' -> yield! v
                                                                                     | _ -> () ])
                                                      (!predicates).[key], 0, link
                                                  | _ -> failwith "Map/where received invalid changes")
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

                      // Update the results dictionary and collect removed keys
                      let deletions =
                        List.map_concat (function
                                           | [] -> []
                                           | [DictDiff (key, v)] ->
                                               match v with
                                               | [Added (VBool true)] -> results.[key] <- parentDict.[key]
                                                                         []
                                               | [Added (VBool false)] -> results.Remove(key) |> ignore
                                                                          [RemovedKey key]
                                               | other -> failwithf "Map's where expects only added vbool changes: %A" other
                                           | other -> failwithf "Dictionary expects all diffs to be of type DictDiff but received %A" other)
                                        predChanges

                      // Ignore changes to keys that are filtered out
                      let containedChanges = [ for change in parentChanges do
                                                 match change with
                                                 | DictDiff (key, v) when results.ContainsKey(key) ->
                                                     results.[key] <- parentDict.[key]
                                                     yield change
                                                 | _ -> () ]
                      Some (op.Children, deletions@containedChanges)),
                   [], contents = VDict results)

    { whereOp with
        Children = dictOp.Children;
        Contents = dictOp.Contents }


let makeDictSelect projectorBuilder uid prio parents =
    let projectors = ref Map.empty
    let results = Dictionary<value, value>()

    let rec buildProjCircuit key env =
      let initial, final = projectorBuilder prio env
      projectors := (!projectors).Add(key, initial)

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
                                                                 [(!projectors).[key], 0, link]
                                                             | RemovedKey key -> []
                                                             | _ -> failwith "Map/where received invalid changes")
                                                          parentChanges
                        Some (List<_> ((dictOp, 0, id)::projsToEval), parentChanges)),
                     parents)

    and dictOp = Operator.Build(uid + "_dict", prio + 0.9,
                   (fun op changes ->
                      // changes.Head contains the dictionary changes passed to the where
                      // changes.Tail contains the changes in the inner predicates
                      let parentChanges, projChanges = changes.Head, changes.Tail

                      List.iteri (fun i chg ->
                                    match chg with
                                    | [] -> ()
                                    | [DictDiff (key, _)] -> results.[key] <- op.Parents.[i + 1].Value // TODO remove values that didn't change
                                    | _ -> failwithf "Dictionary expects all diffs to be of type DictDiff but received %A" chg)
                                  projChanges
                      let deletions = [ for diff in parentChanges do
                                          match diff with
                                          | RemovedKey key -> results.Remove(key) |> ignore
                                                              yield diff
                                          | _ -> () ]
                      Some (op.Children, (List.map_concat id projChanges)@deletions)),
                   [selectOp], contents = VDict results)

    { selectOp with
        Children = dictOp.Children;
        Contents = dictOp.Contents }
