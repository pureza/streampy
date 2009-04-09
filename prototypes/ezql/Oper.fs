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
let connect (parent:oper) (child:oper) fn =
    let index = child.Parents.Count
    
    // parent is the ith input of child
    parent.Children.Add(child, index, fn)
    
    // child knows who is its ith parent
    child.Parents.Insert(index, parent)


// Changes the current value of a continuous value if the new value differs
// from the current one. Also gets the list of changes to propagate.
let setValueAndGetChanges (op:oper) v =
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

type evalStack = (oper * (int * changes) list) list

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
        
    let rec fillLeftArgs (op:oper) inputs idx : changes list =
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

let unaryOp uid prio receiver initial = 
  { Eval = (fun oper inputs -> match inputs with
                               | [v] -> receiver oper v
                               | _ -> failwith "Wrong number of arguments!")
    Children = List<_> ()
    Parents = List<_> ()
    Contents = ref initial
    Priority = prio
    Uid = uid }
                    
let binaryOp uid prio receiver initial =
  { Eval = (fun oper inputs -> match inputs with
                               | [right; left] -> receiver oper right left
                               | _ -> failwith "Wrong number of arguments!")
    Children = List<_> ()
    Parents = List<_> ()
    Contents = ref initial
    Priority = prio
    Uid = uid }

let stream uid prio =
    unaryOp uid prio (fun op changes -> match changes with
                                        | [Added (VEvent ev)] -> Some (op.Children, changes)
                                        | _ -> failwithf "Invalid input: %A" changes)
            VNull


let where uid prio predExpr =
    let arg = match predExpr with
                    | FuncCall (_, [Id (Identifier arg)]) -> arg
                    | _ -> failwith "Invalid predicate to where"

    { Eval = (fun op inputs -> let env = Map.of_list [ for p in op.Parents -> (p.Uid, p.Value) ]
                                           |> Map.remove op.Parents.[0].Uid // remove the stream, because the predicate doesn't depend on it.
                               match inputs with
                               | [Added (VEvent ev)]::_ ->
                                   let env' = env |> Map.add arg (VEvent ev)
                                   match eval env' predExpr with
                                   | VBool true -> Some (op.Children, inputs.Head)
                                   | VBool false -> None
                                   | _ -> failwith "Predicate was supposed to return VBool"
                               | _ -> failwithf "Wrong number of arguments! %A" inputs)
      Children = List<_> ()
      Parents = List<_> ()
      Contents = ref VNull
      Priority = prio
      Uid = uid }
    
let last uid prio field =
    unaryOp uid prio (fun op changes -> match changes with
                                        | [Added (VEvent ev)] -> setValueAndGetChanges op ev.[field]
                                        | _ -> failwithf "Invalid input: %A" changes)
            VNull

let adder uid prio =
    binaryOp uid prio (fun op rightChg leftChg -> let v = op.Parents.[0].Value + op.Parents.[1].Value
                                                  setValueAndGetChanges op v)
             VNull

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
  

(*
let constant uid prio expr =
  let rec eval = function
                 | BinaryExpr (oper, expr1, expr2) -> 
                     let v1 = eval expr1
                     let v2 = eval expr2
                     evalOp (oper, v1, v2)
                 | Integer i -> VInt i
                 | other -> failwithf "This expression is supposed to contain only bin exprs and var refs, but here is %A" other

  and evalOp = function
  | Plus, v1, v2 -> v1 + v2
  | Minus, v1, v2 -> v1 - v2
  | Times, v1, v2 -> v1 * v2
  | _ -> failwith "op not implemented"

  { Eval = (fun _ _ -> None)
    Children = List<_> ()
    Parents = List<_> ()
    Contents = ref (eval expr)
    Priority = prio
    Uid = uid }
  *) 

let arithm uid prio expr =
  let rec eval env = function
                     | BinaryExpr (oper, expr1, expr2) -> 
                         let v1 = eval env expr1
                         let v2 = eval env expr2
                         evalOp (oper, v1, v2)
                     | Integer i -> VInt i
                     | Id (Identifier uid) -> match Map.tryfind uid env with
                                              | Some v -> v
                                              | _ -> failwithf "Cannot find variable %A in the environment" uid
                     | other -> failwithf "This expression is supposed to contain only bin exprs and var refs, but here is %A" other
  and evalOp = function
  | Plus, v1, v2 -> v1 + v2
  | Minus, v1, v2 -> v1 - v2
  | Times, v1, v2 -> v1 * v2
  | _ -> failwith "op not implemented"

  { Eval = (fun oper allChanges -> 
             let env = Map.of_list [ for p in oper.Parents -> (p.Uid, p.Value) ]
             let result = eval env expr
             setValueAndGetChanges oper result)
    Children = List<_> ()
    Parents = List<_> ()
    Contents = ref (try eval Map.empty expr
                    with 
                      | _ -> VNull)
    Priority = prio
    Uid = uid }
