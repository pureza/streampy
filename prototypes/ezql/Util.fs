#light

open Types
open Ast

let toSeconds value unit =
  match unit with
  | Sec -> value
  | Min -> value * 60


// Changes the current value of a continuous value if the new value differs
// from the current one. Also gets the list of changes to propagate.
let setValueAndGetChanges (op:Operator) v =
  if v <> op.Value
    then op.Value <- v
         Some (op.Children, [Added v])
    else None


let getOperEnv op = Map.of_list [ for p in op.Parents -> (p.Uid, p) ]

let convertClosureSpecial v =
  match v with
  | VClosureSpecial (_, lambda, _, context, itself) ->
      let env = Map.map (fun k (v:Operator) -> v.Value) !context
      VClosure (env, lambda, itself)
  | _ -> v

let getOperEnvValues = getOperEnv >> Map.map (fun k v -> v.Value)

let eventTimestamp ev =
  let timestamp = VString "timestamp"
  match ev with
  | VRecord fields when Map.contains timestamp fields -> !fields.[timestamp]
  | _ -> failwithf "Event is not a record or doesn't contain the timestamp field: %A" ev


let retry fn rescue =
    try
      fn ()
    with
      | err -> rescue err
      
let entityDict entity = sprintf "$%s_all" entity

let rec incorporateChanges changes value =
  List.fold (fun value x -> incorporateChange x value) value changes

and incorporateChange change value =
   match change with
   | Added v -> v.Clone()
   | Expired _ -> value
   | RecordDiff (field, changes) ->
       match value with
       | VRecord record -> record.[field] := incorporateChanges changes !record.[field]
                           value
       | _ -> failwithf "%A is not a record!" value
   | DictDiff (key, changes) ->
       match value with
       | VDict dict ->
           let v' = if (!dict).ContainsKey(key)
                      then incorporateChanges changes (!dict).[key]
                      else incorporateChanges changes VNull
           dict := Map.add key v' !dict
           VDict dict
       | _ -> failwithf "%A is not a dictionary!" value
   | RemovedKey key ->
       match value with
       | VDict dict -> dict := Map.remove key !dict
                       VDict dict
       | _ -> failwithf "%A is not a dictionary!" value
       
let rebuildValue changes = List.fold (fun value changes -> incorporateChanges changes value) VNull changes
