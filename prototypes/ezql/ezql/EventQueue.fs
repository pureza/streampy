#light

module EventQueue

    open System
    open System.Collections.Generic
    open System.Timers

    type EventQueue() =
        let queue = SortedList<DateTime, (unit -> unit)>()
        let timer = new Timer(Enabled = true, AutoReset = false)
        let ReSchedule() = if queue.Count <> 0
                               then timer.Interval <- float (queue.Keys.[0] - DateTime.Now).TotalMilliseconds
        
        do timer.Elapsed.Add(fun _ -> queue.Values.[0] ()
                                      queue.RemoveAt 0
                                      ReSchedule ())
                                      
        member self.Register(time, callback) = queue.Add (time, callback)
                                               ReSchedule ()

    let instance = EventQueue ()
