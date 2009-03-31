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
    | VNull
    | VStream of IStream<IEvent>
    | VContinuousValue of IContValue
//    | VMap of IMap
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
        
    static member (*)(left:value, right:value) =
        match left, right with
        | VInteger l, VInteger r -> VInteger (l * r)
        | _ -> failwith "Invalid types in +"
        
    static member op_GreaterThan(left:value, right:value) =
        match left, right with
        | VInteger l, VInteger r -> VBoolean (l > r)
        | _ -> failwith "Invalid types in +"

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
and 'a IStream =
    abstract Add : 'a -> unit
    abstract Where : ('a -> bool) -> 'a IStream
    abstract Select : ('a -> 'b) -> 'b IStream
    abstract TimeJoin : 'a IStream -> ('a option * 'a option) IStream
//    abstract GroupBy : string * ('a IStream -> IContValue) -> IMap
    abstract OnAdd : ('a -> unit) -> unit
    abstract OnExpire : ('a -> unit) -> unit
    abstract InsStream : 'a IStream
    abstract RemStream : 'a IStream

and IContValue =
    abstract Current : value with get
    abstract OnSet : (DateTime * value -> unit) -> unit
    abstract OnExpire : (DateTime * value -> unit) -> unit
    abstract InsStream : IStream<DateTime * value>
    abstract RemStream : IStream<DateTime * value>
(*    abstract (+) : value -> IContValue
    abstract op_GreaterThan : value -> IContValue
    abstract op_AmpAmp : IContValue -> IContValue   *)

(*
and IMap =
    abstract Item : value -> IContValue with get
    abstract Where : (IContValue -> IContValue) -> IMap
    abstract InsStream : IStream<value>
    abstract RemStream : IStream<value>
*)

and Event(timestamp:DateTime, fields:Map<string, value>) =
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
        Event(timestamp, Map.of_list [("value", value)]) :> IEvent


and 'a Stream() =
    let triggerAdd, addEvent = Event.create()
    
    interface 'a IStream with
        member self.Add(item) =
            triggerAdd item
        member self.Where(predicate) =
            let result = Stream() :> 'a IStream
            (self :> 'a IStream).OnAdd(fun item -> if predicate item then result.Add(item))
            result
        member self.Select(projector) =
            let result = Stream() :> 'b IStream
            (self :> 'a IStream).OnAdd(fun item -> result.Add(projector item))
            result
        member self.TimeJoin(other) =
            let result = Stream () :> ('a option * 'a option) IStream
            let temp = ref None
            let key = result.GetHashCode()
            (self :> 'a IStream).OnAdd (fun item -> match !temp with
                                                    | Some v -> result.Add(Some item, Some v)
                                                                Scheduler.removePendingJoin key
                                                                temp := None
                                                    | _ -> temp := Some item
                                                           Scheduler.addPendingJoin key (fun () -> result.Add(Some item, None)))
            other.OnAdd (fun item -> match !temp with
                                     | Some v -> result.Add(Some v, Some item)
                                                 Scheduler.removePendingJoin key
                                                 temp := None
                                     | _ -> temp := Some item
                                            Scheduler.addPendingJoin key (fun () -> result.Add(None, Some item)))
            result
            //let result = JoinStream<'a>(self, other)
            //(self :> 'a IStream).OnAdd(fun item -> result.Add(item, item))
            //result :> ('a * 'a) IStream

//        member self.GroupBy(field, fn) =
//            ParentMap(self, field, fn) :> IMap
            
        member self.OnAdd(action) = addEvent.Add(action)
        member self.OnExpire(action) = () // Events in a stream don't expire
        member self.InsStream = self :> 'a IStream
        member self.RemStream = Stream () :> 'a IStream //Stream<'a>.NullStream()



(*
and ParentMap(stream:IStream<IEvent>, field:string, fn:(IStream<IEvent> -> IContValue)) =
    // Will contain the keys of new or updated elements
    let istream = Stream () :> IStream<value>
    // Will contain the keys of removed elements
    let rstream = Stream () :> IStream<value>
    let substreams = Dictionary<value, IStream<IEvent>>()
    let resultSet = Dictionary<value, IContValue>()

    let addKey key =
        let stream = Stream ()
        substreams.Add(key, stream)
        resultSet.Add(key, fn stream)

    let add (ev:IEvent) =
        let key = ev.[field]
        if not (substreams.ContainsKey(key)) then
            addKey key
            // This line must come before the following to give ChildMaps
            // the chance to create their continuous values for the
            // where expressions
            istream.Add (ev.[field])
        substreams.[key].Add(ev)   

    do stream.OnAdd (fun ev -> add ev)                    

    interface IMap with            
        member self.Item 
            with get(key) =
               // if not (resultSet.ContainsKey(key)) then addKey key |> ignore
                resultSet.[key]
        member self.Where(predicate) = ChildMap(self, predicate) :> IMap
        member self.InsStream = istream
        member self.RemStream = Stream.NullStream()
        
and ChildMap(parent:IMap, predicate) =
    let istream = Stream () :> IStream<value>
    let rstream = Stream () :> IStream<value>
    let liveSet = Dictionary<value, IContValue>()
    do parent.InsStream.OnAdd 
        (fun ev -> 
             let key = ev
             let value = parent.[key]
             let predValue = predicate value
             predValue.OnSet (fun (t, v) -> 
                match v with
                | VBoolean true -> if not (liveSet.ContainsKey(key)) then
                                       liveSet.[key] <- value
                                       istream.Add(key)
                | _ -> if liveSet.ContainsKey(key) then
                           liveSet.Remove(key) |> ignore
                           rstream.Add(key)))
 
    interface IMap with
        member self.Item
            with get(key) = liveSet.[key]
        member self.Where(predicate') = ChildMap(parent, (fun cv -> ContValueOps.And(predicate(cv), predicate'(cv)))) :> IMap
        member self.InsStream = istream
        member self.RemStream = rstream

*)

and 'a Window(istream:'a IStream, rstream:'a IStream) = 
    let contents = ref []
    do istream.OnAdd (fun ev -> contents := !contents @ [ev])
       rstream.OnAdd (fun ev -> contents := List.tl !contents)
    interface 'a IStream with
        member self.Add(item) = failwith "Events should be added to the istream, not the window."             
        member self.Where(predicate) =
            Window (istream.Where(predicate), rstream.Where(predicate)) :> 'a IStream
        member self.Select(projector) =
            Window<'c> (istream.Select(projector), rstream.Select(projector)) :> 'c IStream
        member self.TimeJoin(other) = failwith "ni"
       // member self.GroupBy(field, fn) = ParentMap(self, field, fn) :> IMap
        member self.OnAdd(action) = istream.OnAdd(action)
        member self.OnExpire(action) = rstream.OnAdd(action)
        member self.InsStream = istream
        member self.RemStream = rstream

    member self.Contents = !contents

and ContValue(stream:IStream<DateTime * value>, ?defaultValue:value) =
    let mutable current = if defaultValue.IsSome
                            then defaultValue.Value
                            else VNull
    let istream = Stream () :> IStream<DateTime * value>
    do stream.OnAdd (fun ev -> let time, newVal = ev
                               if newVal <> current then
                                   current <- newVal
                                   istream.Add(ev))

    interface IContValue with
        member self.Current with get() = current
        member self.OnSet(action) = istream.OnAdd (fun ev -> action(ev))
        member self.OnExpire(action) = () // Continuous values don't expire
        member self.InsStream = istream   
        member self.RemStream = Stream () :> (DateTime * value) IStream

  //      member self.Add(right:IContValue) = failwith "ni"
  (*
        member self.(+)(right:value) =
            ContValue ((self :> IContValue).InsStream.Select (fun ev -> Map.of_list [("value", ev.["value"] + right)])) :> IContValue
        member self.op_GreaterThan(right) =
            ContValue ((self :> IContValue).InsStream.Select (fun ev -> Map.of_list [("value", value.op_GreaterThan(ev.["value"], right))])) :> IContValue
        member self.op_AmpAmp(right) =
            let stream = Stream () :> IStream
            let cv = ContValue (stream) :> IContValue
            let selfC = (self :> IContValue)
            let getBool = function
            | VBoolean v -> v
            | _ -> failwith "ContValue &&: Not booleans!"
            selfC.OnSet (fun (t, v) -> stream.Add (Event.WithValue(t, VBoolean ((getBool selfC.Current) && (getBool right.Current)))))
            right.OnSet (fun (t, v) -> stream.Add (Event.WithValue(t, VBoolean ((getBool selfC.Current) && (getBool right.Current)))))
            cv
*)
    override self.ToString() = sprintf "« %A »" (self :> IContValue).Current

    static member FromStream(stream:IStream<IEvent>, field) =
        ContValue (stream.Select (fun ev -> (ev.Timestamp, ev.[field]))) :> IContValue
        
and ContValueWindow(istream:IStream<DateTime * value>, rstream:IStream<DateTime * value>) as self =
    let cv = ContValue (istream) :> IContValue
    let history = SortedList<DateTime, value>()
    do istream.OnAdd (fun (time, v) -> history.Add(time, v))
       rstream.OnAdd (fun ev -> history.RemoveAt(0))
      
    interface IContValue with
        member self.Current with get() = cv.Current
        member self.OnSet(action) = istream.OnAdd (fun ev -> action(ev))
        member self.OnExpire(action) = rstream.OnAdd (fun ev -> action(ev))
        member self.InsStream = istream
        member self.RemStream = rstream
        
        (*
        member self.(+)(right) =
            let selfC = self :> IContValue
            let projector = fun (ev:IEvent) -> Map.of_list [("value", ev.["value"] + right)]
            ContValueWindow (selfC.InsStream.Select projector,
                             selfC.RemStream.Select projector) :> IContValue
        member self.op_GreaterThan(right) =
            let selfC = self :> IContValue
            let projector = fun (ev:IEvent) -> Map.of_list [("value", value.op_GreaterThan(ev.["value"], right))]
            ContValueWindow (selfC.InsStream.Select(projector),
                             selfC.RemStream.Select(projector)) :> IContValue
        member self.op_AmpAmp(right) = failwith "Ni"
         *)
         
         
    member self.Previous = 
        let index = history.Count - 2
        if index >= 0
            then Some (history.Keys.[index], history.Values.[index])
            else None
            
    override self.ToString () =
       "[ @ " + 
            (List.fold_right (+) [ for pair in history -> sprintf " %A: %A " pair.Key.TotalSeconds pair.Value ] "")
             + "]"


and ContValueOps =

    [<OverloadID("cv_plus_cv")>]
    static member Plus(left:IContValue, right:IContValue) = 
        let join = left.InsStream.TimeJoin(right.InsStream)
        let stream = Stream () :> IStream<DateTime * value>
        join.OnAdd (fun (leftEv, rightEv) -> 
                      match leftEv, rightEv with
                      | Some (t1, v1), Some (t2, v2) -> stream.Add(t1, v1 + v2)
                      | Some (t1, v1), None -> stream.Add(t1, v1 + right.Current)
                      | None, Some (t2, v2) -> stream.Add(t2, left.Current + v2)
                      | _ -> failwith "what?")
        ContValue (stream) :> IContValue
    
    [<OverloadID("cv_plus_value")>]
    static member Plus(left:IContValue, right:value) =
         ContValue (left.InsStream.Select (fun (time, v) -> (time, v + right))) :> IContValue

    [<OverloadID("cv_times_value")>]
    static member Times(left:IContValue, right:value) =
         ContValue (left.InsStream.Select (fun (time, v) -> (time, v * right))) :> IContValue
    
    [<OverloadID("cv_gt_cv")>]
    static member GreaterThan(left:IContValue, right:IContValue) = left
    
    [<OverloadID("cv_gt_value")>]
    static member GreaterThan(left:IContValue, right:value) = left
    
    static member And(left:IContValue, right:IContValue) = left
    (*
        member self.Add(right:value) =
            ContValue ((self :> IContValue).InsStream.Select (fun ev -> Map.of_list [("value", ev.["value"] + right)])) :> IContValue
        member self.op_GreaterThan(right) =
            ContValue ((self :> IContValue).InsStream.Select (fun ev -> Map.of_list [("value", value.op_GreaterThan(ev.["value"], right))])) :> IContValue
        member self.op_AmpAmp(right) =
            let stream = Stream () :> IStream
            let cv = ContValue (stream) :> IContValue
            let selfC = (self :> IContValue)
            let getBool = function
            | VBoolean v -> v
            | _ -> failwith "ContValue &&: Not booleans!"
            selfC.OnSet (fun (t, v) -> stream.Add (Event.WithValue(t, VBoolean ((getBool selfC.Current) && (getBool right.Current)))))
            right.OnSet (fun (t, v) -> stream.Add (Event.WithValue(t, VBoolean ((getBool selfC.Current) && (getBool right.Current)))))
            cv
*)

let toSeconds value unit =
    match unit with
    | Sec -> value
    | Min -> value * 60
    
    

