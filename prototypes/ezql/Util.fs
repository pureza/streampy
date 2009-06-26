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
  let differ = match v with
               | VClosureSpecial _ | VClosure _ -> true
               | _ -> v <> op.Value
  if differ
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
  | VRecord fields when Map.contains timestamp fields -> fields.[timestamp]
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
   | Added v -> match value, change with
                | VWindow contents, Added (VWindow contents') -> (VWindow contents') // TODO: Understand why does this happen
                | VWindow contents, _ -> VWindow (contents @ [v])
                | _ -> v
   | Expired _ -> match value with
                  | VWindow (x::xs) -> VWindow xs
                  | _ -> failwithf "Not a VWindow, but a %A" value
   | RecordDiff (field, changes) ->
       match value with
       | VRecord record -> VRecord (Map.add field (incorporateChanges changes record.[field]) record)
       | _ -> failwithf "%A is not a record!" value
   | DictDiff (key, changes) ->
       match value with
       | VDict dict ->
           let v' = if dict.ContainsKey(key)
                      then incorporateChanges changes dict.[key]
                      else incorporateChanges changes VNull
           VDict (Map.add key v' dict)
       | _ -> failwithf "%A is not a dictionary!" value
   | RemovedKey key ->
       match value with
       | VDict dict -> VDict (Map.remove key dict)
       | _ -> failwithf "%A is not a dictionary!" value
       
let rebuildValue changes = List.fold (fun value changes -> incorporateChanges changes value) VNull changes
