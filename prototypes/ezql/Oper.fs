#light

open System
open System.Collections.Generic
open Types

type diff =
    | Added of value
    | Expired of value
    | DictDiff of value * diff list
    | RecordDiff of value * diff list
    | RemovedKey of value

type changes = diff list
type priority = float
type link = changes -> changes

(*
 * An operator is like a graph node that contains a list of parents and
 * children.
 *
 * An operator knows what to do when it is evaluated with a list of inputs.
 * This evaluation function is called with the following arguments:
 * - op: the operator itself ("this")
 * - list<diff list>: lists the differences for each input (for instance, if
 *                    the first input is a window and the second a value,
 *                    this list could be [[AddedEvent ev1; ExpiredEvent ev2]
 *                                        [Set (VInt 3)]]
 *                    If the eval needs the value of the argument and not just
 *                    the diff, it can obtain it through ParentValue(idx)
 *
 * The evaluation function returns:
 * - What changed during this step of the evaluation (a diff list)
 * - The list of (children, inputIndex) to spread these changes
 *)
[<ReferenceEquality>]
type oper = { Eval: oper -> changes list -> (childData List * changes) option
              Children: List<childData>
              Parents: List<oper>
              Contents: ref<value>
              Priority: priority
              Name: string }
  
            member self.ArgCount with get = self.Parents.Count
            member self.Value
                with get = !self.Contents
                and set(v) = self.Contents := v
                
            interface IComparable with 
              member self.CompareTo(other) = Int32.compare (self.GetHashCode()) (other.GetHashCode())
            static member NameOf(op) = op.Name

and childData = oper * int * link

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
        
    match stack with
    | [] -> ()
    | (op, parentChanges)::xs -> 
        let filledChanges = fillLeftArgs op parentChanges 0
        match op.Eval op filledChanges with
        | Some (children, changes) -> spread (mergeStack xs [ for child, idx, link in children -> (child, [(idx, (link changes))]) ])
        | _ -> spread xs


// Some common operators

let unaryOp receiver initial prio name = 
  { Eval = (fun oper inputs -> match inputs with
                               | [v] -> receiver oper v
                               | _ -> failwith "Wrong number of arguments!")
    Children = List<_> ()
    Parents = List<_> ()
    Contents = ref initial
    Priority = prio
    Name = name }
                    
let binaryOp receiver initial prio name =
  { Eval = (fun oper inputs -> match inputs with
                               | [right; left] -> receiver oper right left
                               | _ -> failwith "Wrong number of arguments!")
    Children = List<_> ()
    Parents = List<_> ()
    Contents = ref initial
    Priority = prio
    Name = name }

let stream prio name =
    unaryOp (fun op changes -> match changes with
                               | [Added (VEvent ev)] -> Some (op.Children, changes)
                               | _ -> failwithf "Invalid input: %A" changes)
            VNull prio name


let where predicate prio name =
    unaryOp (fun op changes -> match changes with
                               | [Added (VEvent ev)] -> if predicate ev then Some (op.Children, changes) else None
                               | _ -> failwithf "Invalid input: %A" changes)
            VNull prio name
                              

let last field prio name =
    unaryOp (fun op changes -> match changes with
                               | [Added (VEvent ev)] -> setValueAndGetChanges op ev.[field]
                               | _ -> failwithf "Invalid input: %A" changes)
            VNull prio name

let adder pri name =
    binaryOp (fun op rightChg leftChg -> let v = op.Parents.[0].Value + op.Parents.[1].Value
                                         setValueAndGetChanges op v)
             VNull pri name

let makeRecord (fields:array<string * oper>) prio name =
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
                   Name = name }
    
  for field, op in fields do
    connect op recordOp id
    result.[VString field] <- op.Value
    
  recordOp
  