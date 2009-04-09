#light

open System
open System.Collections.Generic
open Ast

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
type oper = { Eval: oper -> changes list -> (childData List * changes) option
              Children: List<childData>
              Parents: List<oper>
              Contents: ref<value>
              Priority: priority
              Uid: string }
  
            member self.ArgCount with get = self.Parents.Count
            member self.Value
                with get = !self.Contents
                and set(v) = self.Contents := v
                
            override self.ToString() = sprintf "{ %s, child = %d }" self.Uid self.Children.Count
                
            interface IComparable with 
              member self.CompareTo(other) = Int32.compare (self.GetHashCode()) (other.GetHashCode())

            static member UidOf(op) = op.Uid

and childData = oper * int * link

and Event(timestamp:DateTime, fields:Map<string, value>) =
    member self.Timestamp with get() = timestamp
    member self.Item with get(field) = fields.[field]
    member self.Fields = fields

and context = Map<string, value>

and value =
    | VBool of bool
    | VInt of int
    | VString of string
    | VRecord of Dictionary<value, value>
    | VDict of Dictionary<value, value>
    | VClosure of context * expr
    | VEvent of Event
    | VNull
    
    override self.ToString() =
      let s = match self with
              | VBool b -> b.ToString()
              | VInt v -> v.ToString()
              | VString s -> s
              | VRecord m -> m.ToString()
              | VDict m -> m.ToString()
              | VClosure _ -> "..lambda.."
              | VEvent ev -> ev.ToString()
              | VNull -> "VNull"
      (sprintf "« %s »" s)
    
    static member (+)(left:value, right:value) =
        match left, right with
        | VInt l, VInt r -> VInt (l + r)
        | _ -> failwith "Invalid types in +"

    static member (*)(left:value, right:value) =
        match left, right with
        | VInt l, VInt r -> VInt (l * r)
        | _ -> failwith "Invalid types in *"
        
    static member (-)(left:value, right:value) =
        match left, right with
        | VInt l, VInt r -> VInt (l - r)
        | _ -> failwith "Invalid types in +"

    static member op_GreaterThan(left:value, right:value) =
        match left, right with
        | VInt l, VInt r -> VBool (l > r)
        | _ -> failwith "Invalid types in +"        

and diff =
    | Added of value
    | Expired of value
    | DictDiff of value * diff list
    | RecordDiff of value * diff list
    | RemovedKey of value

and changes = diff list
and priority = float
and link = changes -> changes
