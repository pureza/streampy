#light

module DateTimeExtensions

    open System

    type DateTime with
        member self.TotalSeconds = Int32.of_float(TimeSpan(self.Ticks).TotalSeconds)
