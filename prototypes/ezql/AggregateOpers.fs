#light

open Util
open Types

(* Last: records one field of the last event of a stream *)
let makeLast getField (uid, prio, parents, context) =
  let eval = fun (op, inputs) -> 
               let added = List.tryPick (fun diff -> match diff with
                                                     | Added ev -> Some ev
                                                     | _ -> None)
                                        (List.hd inputs)
               Option.bind (fun ev -> setValueAndGetChanges op (getField ev)) added
 
  Operator.Build(uid, prio, eval, parents, context)


(* Sum *)
let makeSum getField (uid, prio, parents, context) =
  let eval = fun ((op:Operator), inputs) -> 
               let initial = if op.Value = VNull then VInt 0 else op.Value
               let balance = List.fold (fun acc diff -> 
                                          match diff with
                                          | Added v -> value.Add(acc, getField v)
                                          | Expired v -> value.Subtract(acc, getField v)
                                          | _ -> failwithf "Invalid diff in sum: %A" diff)
                                            initial (List.hd inputs)
               setValueAndGetChanges op balance
 
  Operator.Build(uid, prio, eval, parents, context)
  
(* Count *)
let makeCount getField (uid, prio, parents, context) =
  let eval = fun ((op:Operator), inputs) -> 
               let initial = if op.Value = VNull then VInt 0 else op.Value
               let balance = List.fold (fun acc diff -> 
                                          match diff with
                                          | Added v -> value.Add(acc, VInt 1)
                                          | Expired v -> value.Subtract(acc, VInt 1)
                                          | _ -> failwithf "Invalid diff in sum: %A" diff)
                                      initial (List.hd inputs)
               setValueAndGetChanges op balance
 
  Operator.Build(uid, prio, eval, parents, context)