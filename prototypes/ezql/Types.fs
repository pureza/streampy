#light

open System
open System.Collections.Generic

type event(timestamp:DateTime, fields:Map<string, value>) =
    member self.Timestamp with get() = timestamp
    member self.Item with get(field) = fields.[field]
    member self.Fields = fields

and value =
    | VBool of bool
    | VInt of int
    | VString of string
    | VRecord of Dictionary<value, value>
    | VDict of Dictionary<value, value>
    | VEvent of event
    | VNull
    
    override self.ToString() =
      let s = match self with
              | VBool b -> b.ToString()
              | VInt v -> v.ToString()
              | VString s -> s
              | VRecord m -> m.ToString()
              | VDict m -> m.ToString()
              | VEvent ev -> ev.ToString()
              | VNull -> "VNull"
      (sprintf "« %s »" s)
    
    static member (+)(left:value, right:value) =
        match left, right with
        | VInt l, VInt r -> VInt (l + r)
        | _ -> failwith "Invalid types in +"