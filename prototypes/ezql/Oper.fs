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

// Example eval stack:
// b = a * c
//
// [b, [(0, [Set (VInt 5)])
//      (1, [])]
//
// This means operator b is to be evaluated because input 0 (could be either
// 'a' or 'c') was set to 5, while the second input stayed the same.

type evalStack = (Operator * (int * changes) list) list

// Joins two evaluation stacks and sorts them by node priority
let mergeStack (stack:evalStack) (toMerge:evalStack) : evalStack =
    let list2Map list initial merge =
      List.fold_left (fun acc (k, v) -> 
                        let v' = if Map.mem k acc then merge v acc.[k] else v
                        Map.add k v' acc)
                     initial list

    let sortWithOrder = Map.to_list >> List.sort_by (fun (op, _) -> op.Priority)

    let stackMap = Map.of_list stack
    let merged = list2Map toMerge stackMap
                          (fun n o -> let o' = Map.of_list o
                                      list2Map n o' (@)
                                        |> Map.to_list
                                        |> List.sort_by fst)
    
    merged |> sortWithOrder
    
(*
 * This is where any change is propagated throughout the graph.
 *)
let rec spread (stack:evalStack) =
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
   //     printfn "*** Vou actualizar o %A" (op.Uid, op.Priority)
   //     printfn "    Changes = %A" parentChanges
        //printfn "%A" (List.map (fun (o, chgs) -> (o.Uid, chgs)) stack)
        let filledChanges = fillLeftArgs op parentChanges 0
        match op.Eval op filledChanges with
        | Some (children, changes) -> spread (mergeStack xs [ for child, idx, link in children -> (child, [(idx, (link changes))]) ])
        | _ -> spread xs


