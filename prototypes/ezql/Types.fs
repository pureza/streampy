#light

open System
open System.Collections.Generic
open Extensions.DateTimeExtensions
open Ast

exception UnknownId of string

type uid = string


module Priority =
  type priority = Priority of int list
  
  let initial = Priority [0]
  let of_list = Priority

  let next (Priority p) =
    match List.rev p with
    | [] -> failwithf "Can't happen"
    | x::xs -> Priority (List.rev ((x + 1)::xs))
    
  let down (Priority p) = Priority (p @ [0])
  let add (Priority right) (Priority left) = Priority (right @ left)


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
 * - The list of (children, inputIndex, link) to spread these changes
 *)
[<ReferenceEquality>]
type Operator =
  { Uid: uid
    Children: List<ChildData>
    Parents: List<Operator>
    Context: Map<uid, Operator> ref
    Priority: Priority.priority
    AllChanges: changes list ref
    Contents: ref<value>
    Eval: Operator * changes list -> SpreadType }

  member self.ArgCount with get() = self.Parents.Count
  member self.Value
    with get() = !self.Contents
    and set(v) = self.Contents := v

  override self.ToString() = sprintf "{ %s, child = %d }" self.Uid self.Children.Count

  interface IComparable with
    member self.CompareTo(other) = Operators.compare (self.GetHashCode()) (other.GetHashCode())

  (* Creates an operator and connects it to its parents *)
  static member Build(uid, prio, eval, parents, context, ?children, ?contents) =
    let theChildren = match children with
                      | Some ch -> ch
                      | None -> List<_>()
    let theContents = match contents with
                      | Some v -> ref v
                      | None -> ref VNull
    let oper = { Uid = uid
                 Priority = prio
                 Eval = eval;
                 Parents = List<_>(Seq.of_list parents)
                 Children = theChildren
                 Context = context
                 Contents = theContents
                 AllChanges = ref [] }

    List.iteri (fun i parent -> parent.Children.Add((oper, i, id))) parents
    oper

  static member UidOf(op) = op.Uid

and ChildData = Operator * int * link

and SpreadType =
  | SpreadTo of List<ChildData> * changes
  | SpreadChildren of changes
  | Delay of List<ChildData> * changes
  | Nothing

and context = Map<string, value>

and value =
    | VBool of bool
    | VInt of int
    | VFloat of single
    | VString of string
    | VRecord of Map<value, value>
    | VDict of Map<value, value>
    | VClosure of context * expr * string option // The name used by the closure to refer to itself (doesn't belong to the context)
    | VRef of value
    | VNull
    | VUnit
    | VVariant of string * value list
    | VClosureSpecial of string * expr * NetworkBuilder * Map<string, Operator> ref * string option
    | VWindow of value list

    override self.ToString() =
      match self with
      | VBool b -> b.ToString()
      | VInt v -> v.ToString()
      | VFloat f -> f.ToString()
      | VString s -> s
      | VRecord m -> (sprintf "{ %s }" (Map.fold_left (fun acc k v -> acc + (sprintf " :%O = %O," k v)) "" m))
      | VDict m -> 
          let s = Text.StringBuilder ()
          for pair in m do
            s.Append(sprintf " :%O = %O,\n " pair.Key pair.Value) |> ignore
          if s.Length > 0
            then s.Remove(0, 1) |> ignore
                 s.Remove (s.Length - 2, 2) |> ignore
          sprintf "{ %O }" s
      | VClosure _ | VClosureSpecial _ -> "..lambda.."
      | VRef value -> sprintf "@%O" value
      | VVariant (label, metadata) -> sprintf "%s (%s)" label (List.fold (fun acc x -> acc + x.ToString() + " ") "" metadata)
      | VWindow values -> sprintf "[%s]" (List.fold (fun acc v -> sprintf "%s %O" acc v) "" values)
      | VNull -> "(null)"
      | VUnit -> "()"


    static member LogicalOp(left, right, op) =
      match left, right with
        | VBool l, VBool r -> VBool (op l r)
        | VNull, VNull -> VNull
        | VNull, VBool b | VBool b, VNull -> VBool (op false b)
        | _ -> failwithf "Invalid types in call to %A: %A %A" op left right

    static member Add(left, right) = 
      match left, right with
      | VInt l, VInt r -> VInt (l + r)
      | VString l, _ -> VString (l + right.ToString())
      | _, VString r -> VString (left.ToString() + r)
      | VNull, _ | _, VNull -> VNull
      | VFloat l, VInt r -> VFloat (l + (single r))
      | VInt l, VFloat r -> VFloat ((single l) + r)
      | VFloat l, VFloat r -> VFloat (l + r)
      | _ -> failwithf "Invalid types in call to +: %A %A" left right
      
    static member Subtract(left, right) = 
      match left, right with
      | VInt l, VInt r -> VInt (l - r)
      | VNull, _ | _, VNull -> VNull
      | VFloat l, VInt r -> VFloat (l - (single r))
      | VInt l, VFloat r -> VFloat ((single l) - r)
      | VFloat l, VFloat r -> VFloat (l - r)
      | _ -> failwithf "Invalid types in call to -: %A %A" left right
      
    static member Multiply(left, right) = 
      match left, right with
      | VInt l, VInt r -> VInt (l * r)
      | VNull, _ | _, VNull -> VNull
      | VFloat l, VInt r -> VFloat (l * (single r))
      | VInt l, VFloat r -> VFloat ((single l) * r)
      | VFloat l, VFloat r -> VFloat (l * r)
      | _ -> failwithf "Invalid types in call to *: %A %A" left right
      
    static member Div(left, right) = 
      match left, right with
      | VInt l, VInt r -> VInt (l / r)
      | VNull, _ | _, VNull -> VNull
      | VFloat l, VInt r -> VFloat (l / (single r))
      | VInt l, VFloat r -> VFloat ((single l) / r)
      | VFloat l, VFloat r -> VFloat (l / r)
      | _ -> failwithf "Invalid types in call to /: %A %A" left right 
      
    static member Mod(left, right) = 
      match left, right with
      | VInt l, VInt r -> VInt (l % r)
      | VNull, _ | _, VNull -> VNull
      | _ -> failwithf "Invalid types in call to mod: %A %A" left right

    static member GreaterThanOrEqual(left, right) = 
       match left, right with
       | VInt l, VInt r -> VBool (l >= r)
       | VNull, _ | _, VNull -> VNull
       | VFloat l, VInt r -> VBool (l >= (single r))
       | VInt l, VFloat r -> VBool ((single l) >= r)
       | VFloat l, VFloat r -> VBool (l >= r)
       | _ -> failwithf "Invalid types in call to >=: %A %A" left right
      
    static member GreaterThan(left, right) = 
      match left, right with
      | VInt l, VInt r -> VBool (l > r)
      | VNull, _ | _, VNull -> VNull
      | VFloat l, VInt r -> VBool (l > (single r))
      | VInt l, VFloat r -> VBool ((single l) > r)
      | VFloat l, VFloat r -> VBool (l > r)
      | _ -> failwithf "Invalid types in call to >: %A %A" left right
     
    static member LessThanOrEqual(left, right) = 
      match left, right with
      | VInt l, VInt r -> VBool (l <= r)
      | VNull, _ | _, VNull -> VNull
      | VFloat l, VInt r -> VBool (l <= (single r))
      | VInt l, VFloat r -> VBool ((single l) <= r)
      | VFloat l, VFloat r -> VBool (l <= r)
      | _ -> failwithf "Invalid types in call to <=: %A %A" left right
     
    static member LessThan(left, right) = 
      match left, right with
      | VInt l, VInt r -> VBool (l < r)
      | VNull, _ | _, VNull -> VNull
      | VFloat l, VInt r -> VBool (l < (single r))
      | VInt l, VFloat r -> VBool ((single l) < r)
      | VFloat l, VFloat r -> VBool (l < r)
      | _ -> failwithf "Invalid types in call to <: %A %A" left right

    static member Equals(left, right) =
      match left, right with
      | VNull, _ | _, VNull -> VNull
      | VFloat l, VInt r -> VBool (l = (single r))
      | VInt l, VFloat r -> VBool ((single l) = r)
      | _ -> VBool (left = right)

    static member Differ(left, right) =
      match value.Equals(left, right) with
      | VBool b -> VBool (not b)
      | VNull -> VNull
      | other -> failwithf "value.Equals returned %A" other
    
    static member And(left, right) = value.LogicalOp(left, right, (&&))
    static member Or(left, right) = value.LogicalOp(left, right, (||))

and diff =
    | Added of value
    | Expired of value
    | DictDiff of value * diff list
    | RecordDiff of value * diff list
    | VisKeyDiff of value * diff list
    | HidKeyDiff of value * bool * diff list // bool true = was hidden just now

and changes = diff list
and link = changes -> changes

and NetworkBuilder = Priority.priority -> Map<uid, Operator> -> Operator list * Operator

(*
 * Merge two lists of changes
 * Most of the time this operation is a simple append, except when the new
 * changes include a DictDiff or a RecordDiff. If this happens, we have to
 * merge them carefully to avoid repeated DictDiffs for the same key or
 * RecordDiffs for the same field.
 *)
let rec mergeChanges (old:changes) (neu:changes) =
  match neu with
  | x::xs -> match x with
             | VisKeyDiff (key, _) | HidKeyDiff (key, _, _) | RecordDiff (key, _) ->
                 let old' = mergeKeyChanges old x
                 mergeChanges old' xs
             | _ -> mergeChanges (old @ neu) xs
  | [] -> old
  
and mergeKeyChanges old toMerge =
  match old with
  | x::xs -> match x, toMerge with
             | VisKeyDiff (key, changes), VisKeyDiff (key', changes') when key = key' ->
                 (VisKeyDiff (key, changes @ changes'))::xs
             | HidKeyDiff (key, whenHidden, changes), HidKeyDiff (key', whenHidden', changes') when key = key' && whenHidden = whenHidden' ->
                 (HidKeyDiff (key, whenHidden, changes @ changes'))::xs
             | RecordDiff (key, changes), RecordDiff (key', changes') when key = key' ->
                 (RecordDiff (key, changes @ changes'))::xs
             | _ -> x::(mergeKeyChanges xs toMerge)
  | _ -> [toMerge]


// Example eval stack:
// b = a * c
//
// [b, [(0, [Set (VInt 5)])
//      (1, [])]
//
// This means operator b is to be evaluated because input 0 (could be either
// 'a' or 'c') was set to 5, while the second input stayed the same.

type Inputs = (int * changes) list
type EvalStack = (Operator * Inputs) list