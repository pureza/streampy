#light

module EventQueue

    open System
    open System.Collections.Generic
    open System.Timers

    type EventQueue() =
        let queue = SortedList<DateTime, (unit -> unit) list>()
        let timer = new Timer(Enabled = false, AutoReset = false)
        let ReSchedule() = if queue.Count <> 0
                               then timer.Interval <- float (queue.Keys.[0] - DateTime.Now).TotalMilliseconds
                                    timer.Enabled <- true

        do timer.Elapsed.Add(fun _ -> List.iter (fun action -> action()) queue.Values.[0]
                                      queue.RemoveAt 0
                                      ReSchedule ())

        member self.Register(time, callback) = 
            if queue.ContainsKey(time)
                then queue.[time] <- callback::queue.[time]
                else queue.[time] <- [callback]
            ReSchedule()

    let instance = EventQueue ()
