#light

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

  let parentDict = match (List.hd parents).Value with
                       | VDict dict -> dict
                       | _ -> failwithf "Can't happen"   

  let makeInit () =
    let initialChanges = ref (Seq.fold (fun acc (x:KeyValuePair<value, value>) ->
                                          (DictDiff (x.Key, [Added x.Value]))::acc)
                                       [] parentDict)
    fun changes -> let changes' = changes @ !initialChanges
                   initialChanges := []
                   changes'
    
  let initOnce = makeInit ()

  Operator.Build(uid, prio,
    (fun op changes ->
      let env = getOperEnv op
      let parentChanges = initOnce changes.Head
      let predsToEval = 
        [ for chg in parentChanges do
            match chg with
            | DictDiff (key, diff) ->
                if not (Map.mem key !subgroups) then buildSubGroup key env op

                // The link between this operator and the group's circuit ignores everything
                // that is not related to that group.
                let link = (fun changes -> [ for diff in changes do
                                               match diff with
                                               | DictDiff (key', v) when key = key' -> yield! v
                                               | _ -> () ])
                let keyOp = subGroupStartOp key subgroups
                // Manually set the value of the subgroup's initial operator
                keyOp.Value <- parentDict.[key]
                yield subGroupStartOp key subgroups, 0, link
            | RemovedKey (key) -> ()
            | chg -> failwithf "%s received invalid changes: %A" uid chg ]

      Some (List<_> ((dictOp, 0, id)::predsToEval), parentChanges)), parents)

     
let makeGroupby field groupBuilder uid prio parents =
    let substreams = ref Map.empty
    let results = new Dictionary<value, value>()

    // Filter only the changes related to the given key
    let filterKey k =
      List.filter (fun change ->
                     match change with
                     | Added (VEvent ev) | Expired (VEvent ev) -> ev.[field] = k
                     | _ -> false)

    // Collect changes by key returning a list of DictDiff's
    let collectByKey =
      List.fold_left (fun acc change -> 
                        match change with
                        | Added (VEvent ev) | Expired (VEvent ev) ->                                 
                            let key = ev.[field]
                            let v' = if Map.mem key acc then change :: acc.[key] else [change]
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

                                 if not (Map.mem key !substreams)
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
                          spreadUnlessEmpty op allChanges
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
 
    let dictOp = Operator.Build(uid + "_dict", Priority.add prio (Priority.of_list [9]),
                   (fun op changes ->
                      // changes.Head contains the dictionary changes passed to the where
                      // changes.Tail contains the changes in the inner predicates
                      let parentChanges, predChanges = changes.Head, changes.Tail
                      let whereOp = op.Parents.[0]
                      let parentDict = match whereOp.Parents.[0].Value with
                                       | VDict d -> d
                                       | _ -> failwith "The parent of this where is not a dictionary!"

                      (* First, take care of the changes reported by the parent dictionary *)
                      let keys1, changes1 =
                        List.unzip [ for change in parentChanges do
                                       match change with
                                       | DictDiff (key, v) ->
                                           match (subGroupResultOp key predicates).Value, results.ContainsKey(key) with
                                           // Add/modify they key, wether we already contain it or not.
                                           | VBool true, _ -> results.[key] <- parentDict.[key]
                                                              yield key, change
                                           // Remove the key if we contain it.
                                           | VBool false, true -> results.Remove(key) |> ignore
                                                                  yield key, RemovedKey key
                                           | _ -> ()
                                       | RemovedKey key -> // Remove the key if we contain it.
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

                      spreadUnlessEmpty op (changes1 @ changes2)),
                   [], contents = VDict results)
     
    let whereOp = makeHeadOp dictOp predicates predicateBuilder uid prio parents                  
    connect whereOp dictOp id

    { whereOp with
        Children = dictOp.Children;
        Contents = dictOp.Contents }


let makeDictSelect projectorBuilder uid prio parents =
    let projectors = ref Map.empty
    let results = Dictionary<value, value>()

    let dictOp = Operator.Build(uid + "_dict", Priority.add prio (Priority.of_list [9]),
                   (fun op changes ->
                      // changes.Head contains the dictionary changes passed to the select
                      // changes.Tail contains the changes in the inner predicates
                      let parentChanges, projChanges = changes.Head, changes.Tail
                                                       
                      let keys1, changes1 =
                        List.unzip [ for chg in parentChanges do
                                       match chg with
                                       | DictDiff (key, _) when not (results.ContainsKey(key)) ->
                                           results.[key] <- (subGroupResultOp key projectors).Value
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
                                  then results.[key] <- (subGroupResultOp key projectors).Value
                                       yield DictDiff (key, [Added results.[key]])
                            | _ -> failwithf "The predicate was supposed to return a boolean, but instead returned %A" change ]
                 
                      spreadUnlessEmpty op (changes1 @ changes2)),                      
                   [], contents = VDict results)
    
    let selectOp = makeHeadOp dictOp projectors projectorBuilder uid prio parents    
    connect selectOp dictOp id

    { selectOp with
        Children = dictOp.Children;
        Contents = dictOp.Contents }
