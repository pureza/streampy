#light

open Types

(* Last: records one field of the last event of a stream *)
let makeLast field uid prio parents =
  let eval = fun op inputs -> 
               let added = List.first (fun diff -> match diff with
                                                   | Added (VEvent ev) -> Some ev
                                                   | _ -> None)
                                      (List.hd inputs)
               Option.bind (fun (ev:Event) -> setValueAndGetChanges op ev.[field]) added
 
  Operator.Build(uid, prio, eval, parents)


(* Sum: records the sum of some field in a stream *)
let makeSum field uid prio parents =

  let eval = fun (op:Operator) inputs -> 
               let initial = if op.Value = VNull then VInt 0 else op.Value
               let balance = List.fold_left (fun acc diff -> 
                                                match diff with
                                                | Added (VEvent ev) -> value.Add(acc, ev.[field])
                                                | Expired (VEvent ev) -> value.Subtract(acc, ev.[field])
                                                | _ -> failwithf "Invalid diff in sum: %A" diff)
                                            initial (List.hd inputs)
               setValueAndGetChanges op balance
 
  Operator.Build(uid, prio, eval, parents)