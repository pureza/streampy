#light

open System
open System.Collections.Generic
open Microsoft.FSharp.Text.StructuredFormat
open Extensions
open Ast
open Util
open Types

exception SpreadException of Operator * EvalStack * Exception

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

// Joins two evaluation stacks and sorts them by node priority
let mergeStack (stack:EvalStack) (toMerge:EvalStack) : EvalStack =
    let list2Map list initial merge =
      List.fold (fun acc (k, v) -> 
                   let v' = if Map.contains k acc then merge v acc.[k] else v
                   Map.add k v' acc)
                initial list

    let sortWithOrder = Map.to_list >> List.sortBy (fun (op, _) -> op.Priority)

    let stackMap = Map.of_list stack
    let merged = list2Map toMerge stackMap
                          (fun n o -> let o' = Map.of_list o
                                      list2Map n o' (fun n o -> mergeChanges o n)
                                        |> Map.to_list
                                        |> List.sortBy fst)
    
    merged |> sortWithOrder


let checkConsistency op changes =
  op.AllChanges := !op.AllChanges @ [(cloneChanges changes)]
  match op.Value with
  | VClosureSpecial _ | VNull -> ()
  | _ -> let rebuilt = rebuildValue !op.AllChanges
         if op.Value <> rebuilt
           then printfn "Operator %O doesn't meet the 'all changes must be seen' invariant.\n Value is %O\n Rebuilt value gives %O\n Changes propagated: %A\n"
                        op op.Value rebuilt !op.AllChanges 

let toEvalStack children (changes:changes) : EvalStack =
  [ for child, idx, link in children -> (child, [(idx, (link changes))]) ]
    
(*
 * This is where any change is propagated throughout the graph.
 *)
let rec spread (stack:EvalStack) =
    let rec fillLeftArgs (op:Operator) inputs idx : changes list =
        match (List.sortBy fst inputs) with
        | (next, changes)::xs -> // If there were no changes for input idx, cons []
                                 if idx < next
                                   then []::(fillLeftArgs op inputs (idx + 1))
                                   else changes::(fillLeftArgs op xs (idx + 1))
        // If there are no more changes but there are still arguments to be filled...
        | [] when idx < op.ArgCount -> [ for i in [1 .. (op.ArgCount - idx)] -> [] ]
        | _ -> []

    match stack with
    | [] -> ()
    | (op, parentChanges)::xs ->
        //printfn "*** Vou actualizar o %s" op.Uid
        //printfn "    Changes = %A\n" parentChanges
        let filledChanges = fillLeftArgs op parentChanges 0
        try
          match op.Eval (op, filledChanges) with
          | Some (children, changes) ->
              //checkConsistency op changes
              spread (mergeStack xs (toEvalStack children (cloneChanges changes)))
          | _ -> spread xs
        with
          | SpreadException (_, _, _) as ex -> raise ex
          | ex -> raise (SpreadException (op, xs, ex))


