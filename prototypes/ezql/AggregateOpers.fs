#light

open Types

(* Last: records one field of the last event of a stream *)
let makeLast getField uid prio parents =
  let eval = fun op inputs -> 
               let added = List.first (fun diff -> match diff with
                                                   | Added (VEvent ev) -> Some (VEvent ev)
                                                   | _ -> None)
                                      (List.hd inputs)
               Option.bind (fun ev -> setValueAndGetChanges op (getField ev)) added
 
  Operator.Build(uid, prio, eval, parents)


(* Sum *)
let makeSum getField uid prio parents =

  let eval = fun (op:Operator) inputs -> 
               let initial = if op.Value = VNull then VInt 0 else op.Value
               let balance = List.fold_left (fun acc diff -> 
                                                match diff with
                                                | Added v -> value.Add(acc, getField v)
                                                | Expired v -> value.Subtract(acc, getField v)
                                                | _ -> failwithf "Invalid diff in sum: %A" diff)
                                            initial (List.hd inputs)
               setValueAndGetChanges op balance
 
  Operator.Build(uid, prio, eval, parents)