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
            member self.Add(item:'a) = triggerAdd item
            member self.Where(predicate) =
                let result = Stream () :> 'a IStream
                (self :> 'a IStream).OnAdd (fun t -> if predicate t then result.Add(t))
                result
            member self.Select(projector) =
                let result = Stream () :> 'b IStream
                (self :> 'a IStream).OnAdd (fun t -> result.Add(projector t))
                result
            member self.OnAdd(action) = addEvent.Add(action)

//    let print (stream: Stream<'a>) = stream.OnAdd (fun t -> printfn "%A" t)


    type 'a Window(interval:int option) =
        let triggerAdd, addEvent = Event.create()
        let triggerExpire, expireEvent = Event.create()
        member self.OnExpire = expireEvent.Add
        member self.Expire = triggerExpire

        interface 'a IStream with
            member self.Add (item:'a) =
                match interval with
                | None -> ()
                | Some value -> EventQueue.instance.Register(DateTime.Now.AddSeconds(float value),
                                                             fun () -> triggerExpire item)
                triggerAdd item

            member self.Where(predicate) =
                let result = Window (None)
                (self :> 'a IStream).OnAdd (fun t -> if predicate t then (result :> 'a IStream).Add t)
                self.OnExpire (fun t -> if predicate t then result.Expire t)
                result :> 'a IStream

            member self.Select(projector) =
                let result = Window(None)
                (self :> 'a IStream).OnAdd(fun t -> (result :> 'b IStream).Add(projector t))
                self.OnExpire(fun t -> result.Expire(projector t))
                result :> IStream<'b>

            member self.OnAdd(action) = addEvent.Add(action)


        static member FromStream(stream: 'a IStream, interval) =
            let result = Window(Some interval)
            stream.OnAdd (fun ev -> (result :> 'a IStream).Add ev)
            result

(*
    let printWindow (window: Window<'a>) =
        window.OnAdd (fun t -> printfn "+ %A" t)
        window.OnExpire (fun t -> printfn "- %A" t)
*)
