#light

open System.Collections.Generic
open Extensions
open Util
open Types
open Oper

type SubCircuitMap = Map<value, Operator * Operator>

let subGroupStartOp key (subgroups:SubCircuitMap ref) = fst (!subgroups).[key]
let subGroupResultOp key (subgroups:SubCircuitMap ref) = snd (!subgroups).[key]

let spreadUnlessEmpty op changes =
  match changes with
  | [] -> Nothing
  | _ -> SpreadChildren changes


(* Used by the dict.where() and dict.select() methods to give an operator to
   the parameter of the predicate or projector. *)
let makeInitialOp (uid, prio, parents, context) =
  let eval = fun (op:Operator, inputs) ->
     let changes = List.hd inputs
     op.Value <- incorporateChanges changes op.Value
     SpreadChildren changes

  Operator.Build(uid, prio, eval, parents, context)


(*
 * Creates the initial operator used by where and select
 *)     
let makeHeadOp dictOp (subgroups:SubCircuitMap ref) groupBuilder (uid, prio, (parents:Operator list), context) =
  let rec buildSubGroup key env headOp =
    let initials, final = groupBuilder prio env
    let initial = List.hd initials
    subgroups := (!subgroups).Add(key, (initial, final))

    // Connects the resulting node to the dictionary operator.
    // The diff is converted into a DictDiff before being passed to the child.
    connect final dictOp (fun changes -> [DictDiff (key, changes)])
  
  let buildIfNew key op =
    if (Map.contains key !subgroups)
      then false
      else let env = !op.Context
           buildSubGroup key env op
           true
  
  (* Initialize the necessary subgroups and normalize changes *)  
  let rec initNewGroups op changes =
     let parentDict =
         match (parents.Head).Value with
         | VDict dict -> dict
         | _ -> failwithf "Can't happen"   
     
     [ for chg in changes do
         match chg with
         | VisKeyDiff (key, _) ->
             if buildIfNew key op
               then yield! [VisKeyDiff (key, [Added parentDict.[key]])]
               else yield! [chg]
         | HidKeyDiff (key, whenHidden, keyChanges) ->
             if buildIfNew key op
               then assert (match keyChanges with // Initial hidden entries must come with [Added <value>] and
                            | [Added _] -> true   // whenHidden must be false
                            | _ -> false)
                    assert (not whenHidden)
             yield! [chg]
         | Added (VDict dict) ->
             assert ((List.length changes) = 1)
             // This may happen during initialization. We to create the
             // appropriate DictDiffs from the initial dictionary and recurse
             yield! initNewGroups op (Seq.fold (fun acc (x:KeyValuePair<value, value>) ->
                                                 (VisKeyDiff (x.Key, [Added x.Value]))::acc)
                                               [] dict)
         | _ -> yield! [chg] ]


  Operator.Build(uid, prio,
    (fun (op, changes) ->
       let parentChanges' = initNewGroups op changes.Head
       let predsToEval =
         [ for chg in parentChanges' do
                        match chg with
                        | VisKeyDiff (key, _) | HidKeyDiff (key, _, _) ->
                            // The link between this operator and the group's circuit ignores everything
                            // that is not related to that group.
                            let link = (fun changes -> [ for diff in changes do
                                                           match diff with
                                                           | (VisKeyDiff (key', v) | HidKeyDiff (key', _, v)) when key = key' -> yield! v
                                                           | _ -> () ])
                                                           
                            yield subGroupStartOp key subgroups, 0, link
                        | _ -> () ]

       SpreadTo (List<_> ((dictOp, 0, id)::predsToEval), parentChanges')), parents, context)

     
let makeGroupby field groupBuilder (uid, prio, parents, context) =
    let substreams = ref Map.empty
    let results = ref Map.empty

    // Filter only the changes related to the given key
    let filterKey k =
      List.filter (fun change ->
                     match change with
                     | Added (VRecord fields) | Expired (VRecord fields) -> fields.[VString field] = k
                     | _ -> false)

    // Collect changes by key returning a list of DictDiff's
    let collectByKey =
      List.fold (fun acc change -> 
                   match change with
                   | Added (VRecord fields) | Expired (VRecord fields) ->                                 
                       let key = fields.[VString field]
                       let v' = if Map.contains key acc then change :: acc.[key] else [change]
                       Map.add key v' acc
                   | _ -> failwith "Invalid changes")
                Map.empty
        >> Map.to_list
        >> List.map DictDiff

    let rec buildSubGroup key env =
      let initials, final = groupBuilder prio env
      let initial = List.hd initials
      substreams := (!substreams).Add(key, (initial, final))
      connect groupOp initial id
      // Connects the resulting node to the dictionary operator.
      // The diff is converted into a DictDiff before being passed to the child.
      connect final dictOp (fun changes -> [DictDiff (key, changes)])

    and groupOp = Operator.Build(uid, prio,
                    (fun (op, changes) ->
                       let env = !op.Context
                       let children = 
                         [ for change in changes.Head ->
                             match change with
                             | Added (VRecord fields) | Expired (VRecord fields) ->
                                 let key = fields.[VString field]

                                 if not (Map.contains key !substreams)
                                   then buildSubGroup key env

                                 fst (!substreams).[key], key
                             | _ -> failwithf "Invalid arguments for groupby! %A" change ]
                       let childrenNoDup = children |> Set.of_list |> Set.to_list
                       match children with
                       | [] -> Nothing
                       | _ -> let childrenData = List.map (fun (g, k) -> g, 0, filterKey k) childrenNoDup
                              SpreadTo (List<_>((dictOp, 0, collectByKey)::childrenData), changes.Head)),
                    parents, context)

    and dictOp = Operator.Build(uid + "_dict", Priority.add prio (Priority.of_list [9]),
                   (fun (op, changes) ->
                      match changes with
                      | parentChanges::groupChanges -> 
                          let groupChanges' =
                            List.mapi (fun i chg ->
                                         match chg with
                                         | [DictDiff (key, keyChanges)] ->
                                             let added = not ((!results).ContainsKey(key))
                                             let value = op.Parents.[i + 1].Value
                                             results := (!results).Add(key, value)
                                             // If the key was just added, return [Added <value>]. Otherwise just return the original change.
                                             if added then [VisKeyDiff (key, [Added value])]
                                                      else [VisKeyDiff (key, keyChanges)]
                                             //chg
                                         | [] -> []
                                         | _ -> failwithf "Dictionary expects all diffs to be of type DictDiff but received %A" chg)
                                      groupChanges

                          let allChanges =
                            (List.mapi (fun i chg ->
                                             match chg with
                                             | DictDiff (key, keyChanges) when not (Map.contains key !results) ->
                                                 // This happens when:
                                                 // 1. The result of the groupby does not depend on g
                                                 // 2. The group has just been created
                                                 // Simply assign the value of the parent and proceed.
                                                 let parent = snd (!substreams).[key]
                                                 let value = parent.Value
                                                 if value <> VNull
                                                   then results := (!results).Add(key, value)
                                                        [VisKeyDiff (key, [Added (!results).[key]])]
                                                   else []
                                             | _ -> [])
                                        parentChanges |> List.concat) @ (List.concat groupChanges')
                          op.Value <- VDict !results
                          spreadUnlessEmpty op allChanges
                      | _ -> failwith "Empty changes?"),
                   [groupOp], context, contents = VDict !results)

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
let makeDictWhere predicateBuilder (uid, prio, parents, context) =
    // Contains the initial and final operator for each key's predicate
    let predicates = ref Map.empty
    let results = ref Map.empty
 
    let dictOp = Operator.Build(uid + "_dict", Priority.add prio (Priority.of_list [9]),
                   (fun (op, changes) ->
                      // changes.Head contains the dictionary changes passed to the where
                      // changes.Tail contains the changes in the inner predicates
                      let parentChanges, predChanges = changes.Head, changes.Tail
                      
                      let whereOp = op.Parents.[0]
                      let parentDict = match whereOp.Parents.[0].Value with
                                       | VDict d -> d
                                       | _ -> failwith "The parent of this where is not a dictionary!"

                      // First, take care of the parent changes
                      // These changes are always transmited forward, but the visibility status
                      // may be modified by the predicates below
                      let changesMap1 =
                        // Put the parent changes in a map of diffs
                        [ for change in parentChanges do
                            match change with
                            | VisKeyDiff (key, diff) ->
                                // The key is visible on the parent. If the predicate is true,
                                // pass the diff. Otherwise, pass a HidKeyDiff, only if the diff is not []
                                if (subGroupResultOp key predicates).Value = VBool true
                                  then yield (key, change)
                                  else match diff with
                                       | [] -> ()
                                       | _ -> yield key, HidKeyDiff (key, Map.contains key !results, diff)
                            | HidKeyDiff (key, true, []) ->
                                // The parent is signaling that it is now hiding the given key.
                                // If we were already hiding it, don't do anything. Otherwise, spread the change.
                                if (Map.contains key !results)
                                  then yield key, change
                            | HidKeyDiff (key, _, diff) -> yield key, HidKeyDiff (key, Map.contains key !results, diff)
                            | _ -> failwithf "dict.where: Invalid changes coming from the parent: %A" change ] |> Map.of_list

                      // Handle changes to visibility status.
                      let changesMap2 =
                        List.fold (fun acc changes ->
                                     match changes with
                                     | [DictDiff (key, [Added (VBool true)])] ->
                                         let change' = match Map.tryFind key acc with
                                                       | Some (VisKeyDiff _ as change) -> None // Visible in the parent, visible here
                                                       | Some (HidKeyDiff _ as change) -> Some change // Invisible in the parent, invisible here
                                                       | Some _ -> failwithf "dict.where: can't happen"
                                                       | None -> // The value itself didn't change, only the predicate did.
                                                                 // If it is visible in the parent, make it visible. Otherwise, don't do anything.
                                                                 if Map.contains key parentDict
                                                                   then Some (VisKeyDiff (key, []))
                                                                   else None
                                         match change' with
                                         | Some chg -> acc.Add(key, chg)
                                         | None -> acc
                                     | [DictDiff (key, [Added (VBool false)])] ->
                                         let change' = match Map.tryFind key acc with
                                                       | Some (HidKeyDiff _ as change) -> None // Invisible in the parent, invisible here
                                                       | Some (VisKeyDiff (key, innerChanges)) -> Some (HidKeyDiff (key, true, innerChanges)) // Was visible but now is hidden
                                                       | Some _ -> failwithf "dict.where: can't happen"
                                                       | None -> // The value itself didn't change, only the predicate did.
                                                                 // If we are already hiding the entry, don't do anything. Otherwise, make it invisible.
                                                                 if Map.contains key !results
                                                                   then Some (HidKeyDiff (key, true, []))
                                                                   else None
                                         match change' with
                                         | Some chg -> acc.Add(key, chg)
                                         | None -> acc
                                     | [] -> acc                                            
                                     | _ -> failwithf "dict.where: invalid changes received from the predicates: %A" changes)
                                   changesMap1 predChanges
                      
                      // Apply the changes
                      for pair in changesMap2 do
                        match pair.Value with
                        | VisKeyDiff (key, _) -> results := (!results).Add(key, parentDict.[key])
                        | HidKeyDiff (key, _, _) -> results := (!results).Remove(key)
                        | _ -> failwithf "Won't happen at this point."
                        
                      let allChanges = changesMap2 |> Map.to_list |> List.map snd

                      op.Value <- VDict !results
                      spreadUnlessEmpty op allChanges),
                   [], context, contents = VDict !results)
     
    let whereOp = makeHeadOp dictOp predicates predicateBuilder (uid, prio, parents, context)
    connect whereOp dictOp id

    { whereOp with
        Children   = dictOp.Children;
        Contents   = dictOp.Contents;
        AllChanges = dictOp.AllChanges }


let makeDictSelect projectorBuilder (uid, prio, parents, context) =
    let projectors = ref Map.empty
    let visible = ref Map.empty
    let hidden = ref Map.empty

    let dictOp = Operator.Build(uid + "_dict", Priority.add prio (Priority.of_list [9]),
                   (fun (op, changes) ->
                      // changes.Head contains the dictionary changes passed to the select
                      // changes.Tail contains the changes in the inner predicates
                      let parentChanges, projChanges = changes.Head, changes.Tail

                      let changesMap1 =
                        // Put the projector changes in a map of DictDiffs
                        [ for changes in projChanges do
                            match changes with
                            | [DictDiff (key, innerChanges) as change] -> yield (key, change)
                            | [] -> ()
                            | _ -> failwithf "dict.select: Invalid changes coming from the projector: %A" changes ] |> Map.of_list

                      let changesMap2 =
                         // Modify the DictDiffs from the map above to either VisKeyDiffs or HidKeyDiffs
                         // There are two corner cases here:
                         // - There was a parent change for a projector that didn't change:
                         //     in this case, we check if the visibility status changed. If it did,
                         //     we propagate it (with [] as inner changes). Otherwise, don't do anything.
                         //     if the value was just added, propagate Added <value> as the inner change.
                         // - There was a projector change and not a parent change:
                         //     in this case, spread a change with the current visibility status.
                         List.fold (fun acc change ->
                                      let change' = match change with
                                                    | VisKeyDiff (key, _) ->
                                                        match Map.tryFind key acc with
                                                        | Some (DictDiff (key, changes)) -> Some (key, VisKeyDiff (key, changes))
                                                        | Some _ -> failwithf "dict.select: can't happen"
                                                        | None -> let exists = Map.contains key !visible || Map.contains key !hidden
                                                                  if exists
                                                                    then if Map.contains key !hidden // Visibility changed
                                                                           then Some (key, VisKeyDiff (key, [Added (subGroupResultOp key projectors).Value]))
                                                                           else None // Was visible, stays visible, the projector didn't change: nothing to do
                                                                    else // This may happen when the key is created for the first time.
                                                                         Some (key, VisKeyDiff (key, [Added (subGroupResultOp key projectors).Value]))
                                                    | HidKeyDiff (key, whenHidden, _) ->
                                                        match Map.tryFind key acc with
                                                        | Some (DictDiff (key, changes)) -> Some (key, HidKeyDiff (key, whenHidden, changes))
                                                        | Some _ -> failwithf "dict.select: can't happen"
                                                        | None -> let exists = Map.contains key !visible || Map.contains key !hidden
                                                                  if exists
                                                                    then if Map.contains key !visible // Visibility changed
                                                                           then assert whenHidden
                                                                                Some (key, HidKeyDiff (key, whenHidden, []))
                                                                           else None // Was hidden, stays hidden, the projector didn't change: nothing to do
                                                                    else Some (key, HidKeyDiff (key, whenHidden, [Added (subGroupResultOp key projectors).Value]))
                                                    | _ -> failwithf "dict.select: Invalid changes coming from the parent: %A" change
                                      match change' with
                                      | Some (key, chg) -> acc.Add(key, chg)
                                      | None -> acc)
                                   changesMap1 parentChanges
                            
                      // Convert remaining DictDiffs to either VisKeyDiff or HidKeyDiff             
                      let changesMap3 = Map.map (fun k v ->
                                                   match v with
                                                   | DictDiff (key, innerChanges) ->
                                                       assert (Map.contains key !visible || Map.contains key !hidden)
                                                       if Map.contains key !visible
                                                         then VisKeyDiff (key, innerChanges)
                                                         else HidKeyDiff (key, false, innerChanges)
                                                   | _ -> v)
                                                changesMap2
                      
                      // Now, apply the changes map
                      for pair in changesMap3 do
                        match pair.Value with
                        | VisKeyDiff (key, _) ->
                            hidden := (!hidden).Remove(key)
                            visible := (!visible).Add(key, (subGroupResultOp key projectors).Value)
                        | HidKeyDiff (key, _, _) ->
                            visible := (!visible).Remove(key)
                            hidden := (!hidden).Add(key, (subGroupResultOp key projectors).Value)
                        | _ -> failwithf "Won't happen at this point."
                        
                      let allChanges = changesMap3 |> Map.to_list |> List.map snd

                      op.Value <- VDict !visible
                      spreadUnlessEmpty op allChanges),
                   [], context, contents = VDict !visible)
    
    let selectOp = makeHeadOp dictOp projectors projectorBuilder (uid, prio, parents, context)
    connect selectOp dictOp id

    { selectOp with
        Children   = dictOp.Children;
        Contents   = dictOp.Contents;
        AllChanges = dictOp.AllChanges }



let makeValues (uid, prio, parents, context) =
  let myDict = ref Map.empty

  let eval = fun (op, allChanges) ->
               let parentDict = match op.Parents.[0].Value with
                                | VDict d -> d
                                | _ -> failwithf "The parent of values() is not a dictionary."
                            
               let myChanges =
                 List.fold (fun myChanges change ->
                                match change with
                                | VisKeyDiff (key, _) ->
                                    let newV = parentDict.[key]
                                    let myChanges' = myChanges @ (if Map.contains key !myDict
                                                                    then [Expired (!myDict).[key]; Added newV]
                                                                    else [Added newV])
                                    myDict := (!myDict).Add(key, newV)
                                    myChanges'
                                | HidKeyDiff (key, true, _) ->
                                    let myChanges' = myChanges @ [Expired (!myDict).[key]]
                                    myDict := Map.remove key (!myDict)
                                    myChanges'
                                | HidKeyDiff (key, false, _) -> myChanges
                                | Added (VDict dict) ->
                                    // Expire the current dictionary
                                    let myChanges' = myChanges @ [ for pair in !myDict ->
                                                                     Expired pair.Value ]
                                    myDict := Map.empty
                                    myChanges' @ [ for pair in dict ->
                                                     myDict := (!myDict).Add(pair.Key, pair.Value)
                                                     Added pair.Value ]
                                | _ -> failwithf "Can't happen... oh really: %A" change)
                           [] (List.hd allChanges)

               op.Value <- VWindow [ for pair in (!myDict) -> pair.Value ]
               SpreadChildren myChanges

  Operator.Build(uid, prio, eval, parents, context, contents = VWindow [])