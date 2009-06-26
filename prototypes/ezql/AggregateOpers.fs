#light

open Util
open Types

(* Last: records one field of the last event of a stream *)
let makeLast getField (uid, prio, parents, context) =
  let eval = fun (op, inputs) -> 
               let added = List.tryPick (fun diff -> match diff with
                                                     | Added (VWindow contents) ->
                                                         match contents with
                                                         | [] -> None
                                                         | _ -> Some contents.[contents.Length - 1]
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
                                          | Added (VWindow contents) ->
                                              // Initialization
                                              List.fold (fun acc v -> value.Add(acc, getField v)) (VInt 0) contents
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
                                          | Added (VWindow contents) -> VInt contents.Length
                                          | Added v -> getField v |> ignore // assert that v is what we're waiting for
                                                       value.Add(acc, VInt 1)
                                          | Expired v -> getField v |> ignore
                                                         value.Subtract(acc, VInt 1)
                                          | _ -> failwithf "Invalid diff in sum: %A" diff)
                                      initial (List.hd inputs)
               setValueAndGetChanges op balance
 
  Operator.Build(uid, prio, eval, parents, context)
  
(* Max *)
let makeMax getField (uid, prio, (parents:Operator list), context) =
  let getWindowMax = function
    | VWindow contents ->
        match contents with
        | [] -> VNull
        | _ -> List.max (List.map getField contents)
    | other -> failwithf "Expecting a VWindow, but got a %A instead." other

  let eval = fun ((op:Operator), inputs) -> 
               let current = op.Value
               let nextValue = List.fold (fun current diff -> 
                                            match diff with
                                            | Added (VWindow contents as window) -> getWindowMax window // Initialization
                                            | Added v -> let newValue = getField v
                                                         if current = VNull || value.GreaterThan(newValue, current) = VBool true
                                                           then newValue
                                                           else current
                                            | Expired v -> let expiredValue = getField v
                                                           if expiredValue = current then getWindowMax parents.[0].Value else current
                                            | _ -> failwithf "Invalid diff in max: %A" diff)
                                      current (List.hd inputs)
               setValueAndGetChanges op nextValue
 
  Operator.Build(uid, prio, eval, parents, context)


(* Min *)
let makeMin getField (uid, prio, (parents:Operator list), context) =
  let getWindowMin = function
    | VWindow contents ->
        match contents with
        | [] -> VNull
        | contents' -> List.min (List.map getField contents')
    | other -> failwithf "Expecting a VWindow, but got a %A instead." other

  let eval = fun ((op:Operator), inputs) -> 
               let current = op.Value
               let nextValue = List.fold (fun current diff -> 
                                            match diff with
                                            | Added (VWindow contents as window) -> getWindowMin window // Initialization
                                            | Added v -> let newValue = getField v
                                                         if current = VNull || value.LessThan(newValue, current) = VBool true
                                                           then newValue
                                                           else current
                                            | Expired v -> let expiredValue = getField v
                                                           if expiredValue = current then getWindowMin parents.[0].Value else current
                                            | _ -> failwithf "Invalid diff in min: %A" diff)
                                      current (List.hd inputs)
               setValueAndGetChanges op nextValue
 
  Operator.Build(uid, prio, eval, parents, context)
  
(* Avg *)
let makeAvg getField (uid, prio, parents, context) =
  let sum = ref (VInt 0)
  let count = ref (VInt 0)
  
  let eval = fun ((op:Operator), inputs) ->
               for change in List.hd inputs do
                 match change with
                 | Added (VWindow contents as window) ->
                     sum := List.fold (fun acc v -> value.Add(acc, getField v)) (VInt 0) contents
                     count := VInt (contents.Length)
                 | Added v -> let v' = getField v
                              sum := value.Add(!sum, v')
                              count := value.Add(!count, VInt 1)
                 | Expired v -> let v' = getField v
                                sum := value.Subtract(!sum, v')
                                count := value.Subtract(!count, VInt 1)
                 | _ -> failwithf "Invalid diff in avg: %A" change
                 
               let nextAvg = if !count = VInt 0 then VNull else value.Div(!sum, !count)
               setValueAndGetChanges op nextAvg                              
 
  Operator.Build(uid, prio, eval, parents, context)      