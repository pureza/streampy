#light

open System
open EzqlAst
open System.Collections.Generic
open System.Text

type context = (string * value) list

and value =
    | VInteger of int
    | VBoolean of bool
    | VSym of symbol
    | VTime of int * timeUnit
    | VStream of IStream
    | VContinuousValue of IContValue
    | VClosure of context * expr
    | VEvent of IEvent
    
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

and Event(timestamp:DateTime, fields:Map<string, value>) =
    interface IEvent with
        member self.Timestamp
            with get() = timestamp
        member self.Item
            with get(field) = fields.[field]
            
    override self.ToString() =
       "{ @ " + timestamp.TimeOfDay.ToString() + " " + 
            (List.reduce_right (+) [ for pair in fields -> sprintf " %s: %A " pair.Key pair.Value ])
             + "}"

(*
 * All IStreams have an OnAdd and an OnExpire. This is not incorrect from a
 * modelling point of view (because events in streams don't expire -- in fact
 * they are never added to begin with) but it unifies the handling of
 * streams and windows.
 *)
and IStream =
    abstract Add : IEvent -> unit
    abstract Where : (IEvent -> bool) -> IStream
    abstract Select : (IEvent -> IEvent) -> IStream
    abstract OnAdd : (IEvent -> unit) -> unit
    abstract OnExpire : (IEvent -> unit) -> unit


and IContValue =
    abstract Current : value option with get
    abstract SetCurrent : DateTime * value option -> unit
    abstract OnSet : (DateTime * value option -> unit) -> unit
    abstract OnExpire : (DateTime * value option -> unit) -> unit

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
            (self :> IStream).OnAdd(fun item -> result.Add(projector item))
            result
        member self.OnAdd(action) = addEvent.Add(action)
        member self.OnExpire(action) = () // Events in a stream don't expire


and Window(interval:int, istream:IStream) as self =  
    let triggerExpire, expireEvent = Event.create()     
    // Should this window keep its contents?
    // Be default -- yes. But if another window is created from this one,
    // then we should not keep contents here.
    let mutable shouldKeep = true  
    do istream.OnAdd(fun item -> (self :> IStream).Add(item)) // Register with istream
       
    interface IStream with
        member self.Add(item) =
            if shouldKeep 
                then EventQueue.instance.Register(DateTime.Now.AddSeconds(float(interval)),
                                                  fun () -> triggerExpire(item))
        member self.Where(predicate) =
            shouldKeep <- false
            Window (interval, istream.Where(predicate)) :> IStream
        member self.Select(projector) =
            shouldKeep <- false
            Window (interval, istream.Select(projector)) :> IStream
        member self.OnAdd(action) = istream.OnAdd(action)
        member self.OnExpire(action) = expireEvent.Add(action)


and ContValue() =
    let triggerSet, setEvent = Event.create()
    let mutable current = (DateTime.MinValue, None)
    interface IContValue with
        member self.Current
            with get() = snd(current)
            
        member self.SetCurrent(time, value) = current <- (time, value)
                                              triggerSet(time, value)
        member self.OnSet(action) = setEvent.Add(action)
        member self.OnExpire(action) = () // Continuous values don't expire

    static member FromStream(stream:IStream, field) =
        let result = ContValue () :> IContValue
        stream.OnAdd (fun item -> result.SetCurrent(item.Timestamp, Some item.[field]))
        result
           
and ContValueWindow(interval:int) =
    let cv = ContValue () :> IContValue
    let triggerExpire, expireEvent = Event.create()
    let history = SortedList<DateTime, value option>()
    
    interface IContValue with
        member self.Current with get() = cv.Current
        member self.SetCurrent(time, value) =
            cv.SetCurrent(time, value)  
            history.Add(time, value)
            if history.Count > 1 then
                EventQueue.instance.Register(time.AddSeconds(float(interval)),
                    fun () -> let (prevTime, prevValue) = (history.Keys.[0], history.Values.[0])
                              history.RemoveAt(0) |> ignore
                              triggerExpire(prevTime, prevValue))
        member self.OnSet(action) = cv.OnSet(action)
        member self.OnExpire(action) = expireEvent.Add(action)
      
    static member FromContValue(cv: IContValue, interval:int) =
        let window = ContValueWindow(interval) :> IContValue
        cv.OnSet (fun (time, value) -> window.SetCurrent(time, value))
        window

and ContValueSum(onSet, onExpire) as self =
    inherit ContValue()
    do let selfC = self :> IContValue 
       onSet(fun (time, value) ->
                selfC.SetCurrent(time, match selfC.Current, value with
                                       | Some t, Some v -> Some (t + v)
                                       | None, Some v -> Some v
                                       | _ -> None))
       onExpire(fun (time, value) ->
                selfC.SetCurrent(time, match selfC.Current, value with
                                       | Some t, Some v -> Some (t - v)
                                       | None, Some v -> Some v
                                       | _ -> None))
    
    static member FromContValue(cv: IContValue) =
        ContValueSum (cv.OnSet, cv.OnExpire) :> IContValue

    static member FromStream(stream: IStream, field) =
        ContValueSum ((fun action -> stream.OnAdd(fun item -> action(item.Timestamp, Some item.[field]))),
                      (fun action -> stream.OnExpire(fun item -> action(item.Timestamp, Some item.[field]))))

                
let printStream (stream: IStream) = 
    stream.OnAdd (fun t -> printfn "+ %A" t)
    stream.OnExpire (fun t -> printfn "- %A" t)

let printContValue (cv: IContValue) =
    cv.OnSet (fun t -> printfn "new cv value: %A" t) 
    cv.OnExpire (fun t -> printfn "expired cv value: %A" t) 
        
let toSeconds value unit =
    match unit with
    | Sec -> value
    | Min -> value * 60
