#light

open System
open EzqlAst
open System.Collections.Generic
open System.Text
open DateTimeExtensions

type context = Map<string, value>

and value =
    | VInteger of int
    | VBoolean of bool
    | VSym of symbol
    | VTime of int * timeUnit
    | VStream of IStream
    | VContinuousValue of IContValue
    | VMap of IMap
    | VClosure of context * expr
    | VEvent of IEvent
    | VRecord of Map<string, value>
    | VType of string
    
    member self.ContValue () =
        match self with
        | VContinuousValue cv -> cv
        | _ -> failwith "Not a continuous value!"
    
    static member (+)(left:value, right:value) =
        match left, right with
        | VInteger l, VInteger r -> VInteger (l + r)
        | _ -> failwith "Invalid types in +"
        
    static member (-)(left:value, right:value) =
        match left, right with
        | VInteger l, VInteger r -> VInteger (l - r)
        | _ -> failwith "Invalid types in -"

and IEvent =
    abstract Timestamp : DateTime with get
    abstract Item : string -> value with get
    abstract Fields : Map<string, value>
(*
 * All IStreams have an OnAdd and an OnExpire. This is not correct from a
 * modelling point of view (because events in streams don't expire -- in fact
 * they are never added to begin with) but it unifies the handling of
 * streams and windows.
 *)
and IStream =
    abstract Add : IEvent -> unit
    abstract Where : (IEvent -> bool) -> IStream
    abstract Select : (IEvent -> Map<string, value>) -> IStream
    abstract GroupBy : string * (IStream -> IContValue) -> IMap
    abstract OnAdd : (IEvent -> unit) -> unit
    abstract OnExpire : (IEvent -> unit) -> unit
    abstract InsStream : IStream
    abstract RemStream : IStream

and IContValue =
    abstract Current : value with get
    abstract OnSet : (DateTime * value -> unit) -> unit
    abstract OnExpire : (DateTime * value -> unit) -> unit
    abstract InsStream : IStream
    abstract RemStream : IStream

and IMap =
    abstract Item : value -> IContValue with get
    abstract Where : (IContValue -> bool) -> IMap
    abstract InsStream : IStream
    abstract RemStream : IStream

type Event(timestamp:DateTime, fields:Map<string, value>) =
    interface IEvent with
        member self.Timestamp
            with get() = timestamp
        member self.Item
            with get(field) = fields.[field]
        member self.Fields = fields

    override self.Equals(otherObj:obj) =
        let other = unbox<IEvent>(otherObj)
        let selfI = self :> IEvent
        other.Timestamp = selfI.Timestamp && other.Fields = selfI.Fields
        
    override self.ToString() =
       "{ @ " + timestamp.TotalSeconds.ToString() + " " + 
            (List.reduce_right (+) [ for pair in fields -> sprintf " %s: %A " pair.Key pair.Value ])
             + "}"
             
    static member WithValue(timestamp, value) =
        Event(timestamp, Map.of_list [("value", value)])


type Stream() =
    let triggerAdd, addEvent = Event.create()
    
    interface IStream with
        member self.Add(item) =
            triggerAdd item
        member self.Where(predicate) =
            let result = Stream() :> IStream
            (self :> IStream).OnAdd(fun item -> if predicate item then result.Add(item))
            result
        member self.Select(projector) =
            let result = Stream() :> IStream
            (self :> IStream).OnAdd(fun item -> result.Add(Event(item.Timestamp, projector item)))
            result
        member self.GroupBy(field, fn) =
            ParentMap(self, field, fn) :> IMap
            
        member self.OnAdd(action) = addEvent.Add(action)
        member self.OnExpire(action) = () // Events in a stream don't expire
        member self.InsStream = self :> IStream
        member self.RemStream = Stream.NullStream
        
    static member NullStream = Stream () :> IStream

and ParentMap(stream:IStream, field:string, fn:(IStream -> IContValue)) =
    // Will contain the keys of new or updated elements
    let istream = Stream () :> IStream
    // Will contain the keys of removed elements
    let rstream = Stream () :> IStream
    let substreams = Dictionary<value, IStream>()
    let resultSet = Dictionary<value, IContValue>()

    let checkAndAddKey key =
        if not (substreams.ContainsKey(key)) then
            let stream = Stream ()
            substreams.Add(key, stream)
            resultSet.Add(key, fn stream)
            resultSet.[key].OnSet (fun (timestamp, _) -> istream.Add(Event.WithValue (timestamp, key)))
    let add (ev:IEvent) =
        let key = ev.[field]
        checkAndAddKey key
        substreams.[key].Add(ev)

    do stream.OnAdd (fun ev ->
                        add ev
                        istream.Add (Event.WithValue(ev.Timestamp, ev.[field])))

    interface IMap with            
        member self.Item 
            with get(key) =
                if not (resultSet.ContainsKey(key)) then checkAndAddKey key
                resultSet.[key]
        member self.Where(predicate) =
            let realPred = fun (ev:IEvent) -> predicate resultSet.[ev.["value"]]
            let childistream = istream.Where(fun ev -> realPred ev)
            let childrstream = istream.Where(fun ev -> not (realPred ev))
            ChildMap (resultSet, childistream, childrstream) :> IMap
        member self.InsStream = istream
        member self.RemStream = Stream.NullStream

and ChildMap(resultSet, pistream, prstream) =
    let istream = Stream () :> IStream
    let rstream = Stream () :> IStream
    let liveSet = Dictionary<value, IContValue>()
    do pistream.OnAdd (fun ev -> 
                         let key = ev.["value"]
                         if not (liveSet.ContainsKey(key)) then
                             liveSet.[key] <- resultSet.[key]
                             istream.Add(ev))
       prstream.OnAdd (fun ev -> 
                         let key = ev.["value"]
                         if (liveSet.ContainsKey(key)) then 
                             liveSet.Remove(key) |> ignore
                             rstream.Add(ev))
    interface IMap with
        member self.Item
            with get(key) = resultSet.[key]
        member self.Where(predicate) = failwith "ni"
        member self.InsStream = istream
        member self.RemStream = rstream

and Window(istream:IStream, rstream:IStream) = 
    interface IStream with
        member self.Add(item) = failwith "Events should be added to the istream, not the window."             
        member self.Where(predicate) =
            Window (istream.Where(predicate), rstream.Where(predicate)) :> IStream
        member self.Select(projector) =
            Window (istream.Select(projector), rstream.Select(projector)) :> IStream
        member self.GroupBy(field, fn) = ParentMap(self, field, fn) :> IMap
        member self.OnAdd(action) = istream.OnAdd(action)
        member self.OnExpire(action) = rstream.OnAdd(action)
        member self.InsStream = istream
        member self.RemStream = rstream

and ContValue(istream:IStream, ?defaultValue:value) =
    let mutable current = defaultValue
    do istream.OnAdd (fun ev -> current <- Some ev.["value"])

    interface IContValue with
        member self.Current
            with get() = current.Value
        member self.OnSet(action) = istream.OnAdd (fun ev -> action(ev.Timestamp, ev.["value"]))
        member self.OnExpire(action) = () // Continuous values don't expire
        member self.InsStream = istream   
        member self.RemStream = Stream.NullStream                                       

    override self.ToString() = sprintf "« %A »" (self :> IContValue).Current

    static member FromStream(stream:IStream, field) =
        ContValue (stream.Select (fun ev -> Map.of_list [("value", ev.[field])])) :> IContValue

type ContValueWindow(istream:IStream, rstream:IStream) =
    let cv = ContValue (istream) :> IContValue
    let history = SortedList<DateTime, value>()
    do istream.OnAdd (fun ev -> history.Add(ev.Timestamp, ev.["value"]))
       rstream.OnAdd (fun ev -> history.RemoveAt(0))
    
    interface IContValue with
        member self.Current with get() = cv.Current
        member self.OnSet(action) = cv.OnSet(action)
        member self.OnExpire(action) = rstream.OnAdd (fun ev -> action(ev.Timestamp, ev.["value"]))
        member self.InsStream = istream
        member self.RemStream = rstream
         
    member self.Previous = 
        let index = history.Count - 2
        if index >= 0
            then Some (history.Keys.[index], history.Values.[index])
            else None

let toSeconds value unit =
    match unit with
    | Sec -> value
    | Min -> value * 60
