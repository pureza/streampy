#light

module Stream

    open System
    open System.Collections.Generic

    type 'a IStream =
        abstract Add : 'a -> unit
        abstract Where : ('a -> bool) -> 'a IStream
        abstract Select : ('a -> 'b) -> 'b IStream
        abstract OnAdd : ('a -> unit) -> unit

    type 'a Stream() =
        let triggerAdd, addEvent = Event.create()

        interface 'a IStream with
            member self.Add(item) = triggerAdd item
            member self.Where(predicate) =
                let result = Stream () :> 'a IStream
                (self :> 'a IStream).OnAdd (fun t -> if predicate t then result.Add(t))
                result
            member self.Select(projector) =
                let result = Stream () :> 'b IStream
                (self :> 'a IStream).OnAdd (fun t -> result.Add(projector t))
                result
            member self.OnAdd(action) = addEvent.Add(action)

    type 'a Window(interval:int) =
        let triggerAdd, addEvent = Event.create()
        let triggerExpire, expireEvent = Event.create()
        let mutable shouldKeep = true
        member self.OnExpire = expireEvent.Add
        member self.Expire = triggerExpire

        interface 'a IStream with
            member self.Add(item) =
                if shouldKeep then 
                    EventQueue.instance.Register(DateTime.Now.AddSeconds(float interval),
                                                 fun () -> triggerExpire item)                                                 
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


        static member FromStream(stream: 'a IStream, interval) =
            let result = Window(interval) :> 'a IStream
            stream.OnAdd (fun ev -> result.Add ev)
            result

    let print (stream: IStream<'a>) =
        stream.OnAdd (fun t -> printfn "%A" t)
        match stream with
        | :? Window<'a> -> (stream :?> 'a Window).OnExpire (fun t -> printfn "%A" t)
        | _ -> ()