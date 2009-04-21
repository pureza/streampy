#light

open System
open System.Collections.Generic
open Extensions.DateTimeExtensions
open Ast

type uid = string

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
  { Eval: Operator -> changes list -> (ChildData List * changes) option
    Children: List<ChildData>
    Parents: List<Operator>
    Contents: ref<value>
    Priority: priority
    Uid: uid }

  member self.ArgCount with get = self.Parents.Count
  member self.Value
    with get = !self.Contents
    and set(v) = self.Contents := v

  override self.ToString() = sprintf "{ %s, child = %d }" self.Uid self.Children.Count

  interface IComparable with
    member self.CompareTo(other) = Int32.compare (self.GetHashCode()) (other.GetHashCode())

  (* Creates an operator and connects it to its parents *)
  static member Build(uid, prio, eval, parents, ?children, ?contents) =
    let theChildren = match children with
                      | Some ch -> ch
                      | None -> List<_>()
    let theContents = match contents with
                      | Some v -> ref v
                      | None -> ref VNull
    let oper = { Uid = uid; Priority = prio; Eval = eval;
                 Parents = List<_>(Seq.of_list parents);
                 Children = theChildren; Contents = theContents }

    List.iteri (fun i parent -> parent.Children.Add((oper, i, id))) parents
    oper

  static member UidOf(op) = op.Uid

and ChildData = Operator * int * link

and Event(timestamp:DateTime, fields:Map<string, value>) =
    member self.Timestamp with get() = timestamp
    member self.Item
      with get(field) = match Map.tryfind field fields with
                        | Some result -> result
                        | None -> failwithf "This event does not contain the field '%s'" field

    member self.Fields = fields
    
    override self.Equals(otherObj:obj) =
        let other = unbox<Event>(otherObj)
        other.Timestamp = self.Timestamp && self.Fields = other.Fields
    
    override self.ToString() =
       "{ @ " + timestamp.TotalSeconds.ToString() + " " + 
            (List.reduce_right (+) [ for pair in fields -> sprintf " %s: %A " pair.Key pair.Value ])
             + "}"

and context = Map<string, value>

and value =
    | VBool of bool
    | VInt of int
    | VString of string
    | VRecord of Map<value, value ref>
    | VDict of Dictionary<value, value>
    | VClosure of context * expr
    | VEvent of Event
    | VNull

    override self.ToString() =
      let s = match self with
              | VBool b -> b.ToString()
              | VInt v -> v.ToString()
              | VString s -> s
              | VRecord m -> (sprintf "{ %s }" (Map.fold_left (fun acc k v -> acc + (sprintf " :%O = %A," k (!v))) "" m))
              | VDict m -> m.ToString()
              | VClosure _ -> "..lambda.."
              | VEvent ev -> ev.ToString()
              | VNull -> "VNull"
      (sprintf "« %s »" s)

    static member IntArithmOp(left, right, op) =
      match left, right with
        | VInt l, VInt r -> VInt (op l r)
        | _ -> failwithf "Invalid types in call to %A: %A %A" op left right

    static member IntCmpOp(left, right, op) =
      match left, right with
        | VInt l, VInt r -> VBool (op l r)
        | _ -> failwithf "Invalid types in call to %A: %A %A" op left right

    static member Add(left, right) = value.IntArithmOp(left, right, (+))
    static member Multiply(left, right) = value.IntArithmOp(left, right, (*))
    static member Subtract(left, right) = value.IntArithmOp(left, right, (-))
    static member GreaterThan(left, right) = value.IntCmpOp(left, right, (>))
    static member Equals(left, right) = value.IntCmpOp(left, right, (=))


and diff =
    | Added of value
    | Expired of value
    | DictDiff of value * diff list
    | RecordDiff of value * diff list
    | RemovedKey of value

and changes = diff list
and priority = float
and link = changes -> changes


let toSeconds value unit =
  match unit with
  | Sec -> value
  | Min -> value * 60


// Changes the current value of a continuous value if the new value differs
// from the current one. Also gets the list of changes to propagate.
let setValueAndGetChanges (op:Operator) v =
    if v <> op.Value
      then op.Value <- v
           Some (op.Children, [Added v])
      else None