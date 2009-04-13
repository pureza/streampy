#light

open System
open System.Collections.Generic
open Microsoft.FSharp.Text.StructuredFormat
open Ast
open Types
open Eval

(*
 * Connect parent with child.
 * fn transforms the output of oper before it is delivered to child.
 *)
let connect (parent:Operator) (child:Operator) fn =
    let index = child.Parents.Count
    
    // parent is the ith input of child
    parent.Children.Add(child, index, fn)
    
    // child knows who is its ith parent
    child.Parents.Insert(index, parent)


// Changes the current value of a continuous value if the new value differs
// from the current one. Also gets the list of changes to propagate.
let setValueAndGetChanges (op:Operator) v =
    if v <> op.Value
      then op.Value <- v
           Some (op.Children, [Added v])
      else None


// Example eval stack:
// b = a * c
//
// [b, [(0, [Set (VInt 5)])
//      (1, [])]
//
// This means operator b is to be evaluated because input 0 (could be either
// 'a' or 'c') was set to 5, while the second input stayed the same.

type evalStack = (Operator * (int * changes) list) list

(*
 * This is where any change is propagated throughout the graph.
 *)
let rec spread (stack:evalStack) =
    // Joins two evaluation stacks and sorts them by node priority
    let mergeStack (stack:evalStack) (toMerge:evalStack) : evalStack =
        let sortWithOrder = Map.to_list >> List.sort_by (fun (op, _) -> op.Priority) 

        let stackMap = Map.of_list stack
        let merged = List.fold_left (fun acc (k, v) ->
                                       if Map.mem k acc
                                         then Map.add k (acc.[k]@v) acc
                                         else Map.add k v acc) stackMap toMerge
        merged |> sortWithOrder 
        
    let rec fillLeftArgs (op:Operator) inputs idx : changes list =
        match (List.sort_by fst inputs) with
        | (next, changes)::xs -> // If there were no changes for input idx, cons []
                                 if idx < next
                                   then []::(fillLeftArgs op inputs (idx + 1))
                                   else changes::(fillLeftArgs op xs (idx + 1))
        // If there are no more changes but there are still arguments to be filled...
        | [] when idx < op.ArgCount -> [ for i in [1 .. (op.ArgCount - idx)] -> [] ]
        | _ -> []
    
//    for op, changes in stack do
//      printfn "[%O %A]" op changes
//    printfn ""  
    match stack with
    | [] -> ()
    | (op, parentChanges)::xs -> 
        // printfn "*** Vou actualizar o %O" op
        let filledChanges = fillLeftArgs op parentChanges 0
        match op.Eval op filledChanges with
        | Some (children, changes) -> spread (mergeStack xs [ for child, idx, link in children -> (child, [(idx, (link changes))]) ])
        | _ -> spread xs


// Some common operators

(* A stream: propagates received events *)
let makeStream uid prio parents = 
  let eval = fun op inputs -> match inputs with
                              | [[Added (VEvent ev)] as changes] -> Some (op.Children, changes)
                              | _ -> failwith "Wrong number of arguments!"
  
  Operator.Build(uid, prio, eval, parents)


(* Where: propagates events that pass a given predicate *)
let makeWhere predLambda uid prio parents =
    let expr = FuncCall (predLambda, [Id (Identifier "ev")])
    let eval = fun op inputs ->
                 let env = Map.of_list [ for p in op.Parents do
                                           if p <> op.Parents.[0]
                                             then yield (p.Uid, p.Value) ]
                 match inputs with
                 | [Added (VEvent ev)]::_ ->
                     let env' = Map.add "ev" (VEvent ev) env
                     match eval env' expr with
                       | VBool true -> Some (op.Children, inputs.Head)
                       | VBool false -> None
                       | _ -> failwith "Predicate was supposed to return VBool"
                 | _ -> failwithf "Wrong number of arguments! %A" inputs

    Operator.Build(uid, prio, eval, parents)


(* Last: records one field of the last event of a stream *)
let makeLast field uid prio parents =
  let eval = fun op inputs -> match inputs with
                              | [[Added (VEvent ev)] as changes] -> setValueAndGetChanges op ev.[field]
                              | _ -> failwith "Wrong number of arguments!"
  
  Operator.Build(uid, prio, eval, parents)


(* Evaluator: this operator evaluates some expression and records its result *)
let makeEvaluator expr uid prio parents =
  let operEval = fun oper allChanges -> 
                   let env = Map.of_list [ for p in oper.Parents -> (p.Uid, p.Value) ]
                   let result = eval env expr
                   setValueAndGetChanges oper result

  Operator.Build(uid, prio, operEval, parents)


(*
let makeRecord uid (fields:array<string * oper>) prio =
  let result = Dictionary<value, value>()
  
  let recordOp = { Eval = (fun oper allChanges -> 
                             let recordChanges =
                               allChanges
                                 |> List.mapi (fun i changes -> match changes with
                                                                | x::xs -> let field = VString (fst fields.[i])
                                                                           result.[field] <- oper.Parents.[i].Value
                                                                           [RecordDiff (field, changes)]
                                                                | _ -> [])
                                 |> List.map_concat id
                             Some (oper.Children, recordChanges))
                   Children = List<_> ()
                   Parents = List<_> ()
                   Contents = ref (VRecord result)
                   Priority = prio
                   Uid = uid }
    
  for field, op in fields do
    connect op recordOp id
    result.[VString field] <- op.Value
    
  recordOp
 *) 



