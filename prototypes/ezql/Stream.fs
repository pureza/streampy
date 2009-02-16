#light

module Stream

    open System
    open System.Collections.Generic

    type 'a INotifyUpdates =
        abstract OnAdd : ('a -> unit) -> unit
        abstract OnExpire : ('a -> unit) -> unit


    type 'a IStream =
        inherit INotifyUpdates<'a>
        abstract Add : 'a -> unit
        abstract Where : ('a -> bool) -> 'a IStream
        abstract Select : ('a -> 'b) -> 'b IStream


    type 'a IWindow =
        abstract Expire : 'a -> unit


    type 'a Window(interval:int option) =
        let triggerAdd, addEvent = Event.create()
        let triggerExpire, expireEvent = Event.create()
        let mutable shouldKeep = true       

        interface 'a IStream with
            member self.Add(item) =
                if shouldKeep then 
                    match interval with
                    | Some value -> 
                        EventQueue.instance.Register(DateTime.Now.AddSeconds(float value),
                                                     fun () -> triggerExpire item)      
                    | None -> ()                                                                                                
                triggerAdd item
            member self.Where(predicate) =
                let result = Window (interval) :> 'a IStream
                shouldKeep <- false
                (self :> 'a IStream).OnAdd (fun t -> if predicate t then result.Add(t))
                result
            member self.Select(projector) =
                let result = Window(interval) :> 'b IStream
                shouldKeep <- false
                (self :> 'a IStream).OnAdd(fun t -> result.Add(projector t))
                result

            member self.OnAdd(action) = addEvent.Add(action)
            member self.OnExpire(action) = expireEvent.Add(action)
        
        interface 'a IWindow with
            member self.Expire(item) = triggerExpire item  

        static member FromStream(stream: 'a IStream, interval) =
            let result = Window(Some interval) :> 'a IStream
            stream.OnAdd (fun ev -> result.Add ev)
            result

    type 'a ContValue() =
        let triggerAdd, addEvent = Event.create()
        let triggerExpire, expireEvent = Event.create()
        let mutable current = None
        member self.Current
            with get() = current and
                 internal set(value) = current <- Some value
                                       triggerAdd(value)
        static member CreateFrom(parent:INotifyUpdates<'b>, evaluator) =
            let cv = new ContValue<'a>()
            parent.OnAdd (fun item -> cv.Current <- evaluator(cv, item))
            cv
        interface 'a INotifyUpdates with
            member self.OnAdd(action) = addEvent.Add(action)
            member self.OnExpire(action) = expireEvent.Add(action)

    let stream () = Window None

    let print (stream: INotifyUpdates<'a>) =
        stream.OnAdd (fun t -> printfn "%A" t)
        stream.OnExpire (fun t -> printfn "%A" t)