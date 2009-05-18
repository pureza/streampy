#light

open Types

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


let getOperEnv op = Map.of_list [ for p in op.Parents do
                                    if p <> op.Parents.[0]
                                      then yield (p.Uid, p) ]

let getOperEnvValues = getOperEnv >> Map.mapi (fun k v -> v.Value)


let recordToEvent record = Map.fold_left (fun acc k v ->
                                            match k with
                                            | VString k' -> Map.add k' !v acc
                                            | _ -> failwithf "Can't happen! %A" k)
                                         Map.empty record
                                         
                                         
let retry fn rescue =
    try
      fn ()
    with
      | err -> rescue err