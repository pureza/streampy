#light

open System
open EzqlAst
open System.Collections.Generic
open Stream
open System.Text

type context = (string * value) list

and value =
    | Integer of int
    | Boolean of bool
    | Sym of symbol
    | Time of int * timeUnit
    | Stream of IStream<IEvent>
    | Closure of context * expr
    | Event of IEvent

and IEvent =
    abstract Timestamp : DateTime
        with get, set
    abstract Item : string -> value
        with get, set

and Event() =
    let fields = Dictionary<string, value>()
    let mutable timestamp = DateTime.Now
    interface IEvent with
        member self.Timestamp
            with get() = timestamp and
                 set v = timestamp <- v
        member self.Item
            with get field = fields.[field] and
                 set field v = fields.[field] <- v
    override self.ToString() =
        let mutable buffer = StringBuilder("{ @ ")
        buffer <- buffer.Append(timestamp.TimeOfDay)
        for key in fields.Keys do
            buffer <- buffer.Append(sprintf " %s: %A " key fields.[key])
        buffer.Append("}").ToString()

let toSeconds value unit =
    match unit with
    | Sec -> value
    | Min -> value * 60
