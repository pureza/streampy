#light

open Extensions.DateTimeExtensions
open Util
open Types
open Ast
open Eval

(* Last: records one field of the last event of a stream *)
let makeLast getField (uid, prio, parents, context) =
  let eval = fun (op, inputs) -> 
               let added = List.tryPick (fun diff -> match diff with
                                                     | Added (VWindow contents) ->
                                                         match contents with
                                                         | [] -> None
                                                         | _ -> Some contents.[contents.Length - 1]
                                                     | Added ev when getField ev <> VNull -> Some ev
                                                     | _ -> None)
                                        (List.hd inputs)
               match added with
               | None -> Nothing
               | Some ev -> setValueAndGetChanges op (getField ev)
 
  Operator.Build(uid, prio, eval, parents, context)

(* Prev: records one field of the previous event *)
let makePrev getField (uid, prio, parents, context) =
  let curr = ref VNull

  let eval = fun (op, inputs) -> 
               let added = List.tryPick (fun diff -> match diff with
                                                     | Added (VWindow contents) ->
                                                         match contents with
                                                         | [] -> None
                                                         | [x] -> curr := x
                                                                  None
                                                         | _ -> curr := contents.[contents.Length - 1]
                                                                Some contents.[contents.Length - 2]
                                                     | Added ev when getField ev <> VNull ->
                                                         let prev = !curr
                                                         curr := ev
                                                         if prev <> VNull then Some prev else None
                                                     | _ -> None)
                                        (List.hd inputs)

               match added with
               | None -> Nothing
               | Some ev -> setValueAndGetChanges op (getField ev)
 
  Operator.Build(uid, prio, eval, parents, context)

(* Sum *)
let makeSum getField (uid, prio, parents, context) =
  let eval = fun ((op:Operator), inputs) -> 
               let initial = if op.Value = VNull then VInt 0 else op.Value
               let balance = List.fold (fun acc diff -> 
                                          match diff with
                                          | Added (VWindow contents) ->
                                              // Initialization
                                              List.fold (fun acc v -> value.Add(acc, getField v)) (VInt 0) (List.filter (fun v -> v <> VNull) contents)
                                          | Added v when getField v = VNull -> acc
                                          | Added v -> value.Add(acc, getField v)
                                          | Expired VNull -> acc
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
                                          | Added v when getField v = VNull -> acc
                                          | Added v -> getField v |> ignore // assert that v is what we're waiting for
                                                       value.Add(acc, VInt 1)
                                          | Expired VNull -> acc
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
        | _ -> List.max (List.map getField (List.filter (fun v -> v <> VNull) contents))
    | other -> failwithf "Expecting a VWindow, but got a %A instead." other

  let eval = fun ((op:Operator), inputs) -> 
               let current = op.Value
               let nextValue = List.fold (fun current diff -> 
                                            match diff with
                                            | Added (VWindow contents as window) -> getWindowMax window // Initialization
                                            | Added v when getField v = VNull -> current
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
        | _ -> List.min (List.map getField (List.filter (fun v -> v <> VNull) contents))
    | other -> failwithf "Expecting a VWindow, but got a %A instead." other

  let eval = fun ((op:Operator), inputs) -> 
               let current = op.Value
               let nextValue = List.fold (fun current diff -> 
                                            match diff with
                                            | Added (VWindow contents as window) -> getWindowMin window // Initialization
                                            | Added v when getField v = VNull -> current
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
                     sum := List.fold (fun acc v -> value.Add(acc, getField v)) (VInt 0) (List.filter (fun v -> v <> VNull) contents)
                     count := VInt (contents.Length)
                 | Added v when getField v = VNull -> ()  
                 | Added v -> let v' = getField v
                              sum := value.Add(!sum, v')
                              count := value.Add(!count, VInt 1)
                 | Expired VNull -> ()
                 | Expired v -> let v' = getField v
                                sum := value.Subtract(!sum, v')
                                count := value.Subtract(!count, VInt 1)
                 | _ -> failwithf "Invalid diff in avg: %A" change
                 
               let nextAvg = if !count = VInt 0 then VNull else value.Div(!sum, !count)
               setValueAndGetChanges op nextAvg                              
 
  Operator.Build(uid, prio, eval, parents, context)      


(*
 * isAny: true to behave like .any?, false to emulate .all?
 *) 
let makeAnyAll isAny pred (uid, prio, parents, context) =
  let eval = fun ((op:Operator), inputs) ->
               let expr = FuncCall (pred, [Id (Identifier "$v")])
               let window = match op.Parents.[0].Value with
                            | VWindow contents -> Some contents
                            | _ -> None
               let env = getOperEnvValues op

               try
                 let result = match inputs with
                               | (winChanges::rest) when List.exists (function | [] -> false | _ -> true) rest ||
                                                         List.exists (function | Expired _ -> true | _ -> false) winChanges ->
                                   match window with
                                   | Some window' ->
                                       // The predicate changed or there was an expiration: must re-evaluate the entire window
                                       VBool ((if isAny then List.exists else List.forall)
                                                (fun v -> eval (env.Add("$v", v)) expr = VBool true)
                                                (List.filter (fun v -> v <> VNull) window'))
                                   | None -> failwithf ".any?: The predicate changed but I can't re-evaluate past values!"
                               | winChanges::_ ->
                                   // If this is .any?() and the value is already true or is .all?() and
                                   // the value is already false, nothing to do.
                                   if op.Value = VBool isAny
                                     then VBool isAny
                                     else let results = List.map (function
                                                                    | Added v -> try
                                                                                   // An exception may occur if, for example, a field
                                                                                   // of the event is null.
                                                                                   Some (eval (env.Add("$v", v)) expr = VBool true)
                                                                                 with err -> None
                                                                    | other -> failwithf "Can't happen! .any? received %A" other)
                                                                 winChanges
                                          let validResults = List.filter Option.isSome results
                                          match validResults with
                                          | [] -> op.Value
                                          | _ -> let fn = if isAny then List.exists else List.forall
                                                 VBool (fn (function
                                                          | Some b -> b
                                                          | None -> failwithf "Can't happen")
                                                       (List.filter Option.isSome results))
                               | _ -> failwithf "Can't happen: %A" inputs
                 setValueAndGetChanges op result
               with ex -> Nothing                                                                          

  Operator.Build(uid, prio, eval, parents, context)  


(* howLong: for how long was a given condition true *)
let makeHowLong (uid, prio, parents, context) =
  let prevValue = ref false

  let eval = fun (op:Operator, inputs) -> 
               match op.Parents.[0].Value, !prevValue with
               | VBool true, true ->
                   let total = value.Add(op.Value, VInt 1)
                   setValueAndGetChanges op total
               | VBool true, false ->
                   prevValue := true
                   Nothing
               | VBool false, true ->
                   prevValue := false
                   let total = value.Add(op.Value, VInt 1)
                   setValueAndGetChanges op total
               | _ -> Nothing
 
  Operator.Build(uid, prio, eval, parents, context, contents = VInt 0)  