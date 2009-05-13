#light


module DateTimeExtensions =
    open System

    type DateTime with
        member self.TotalSeconds = Int32.of_float(TimeSpan(self.Ticks).TotalSeconds)
        static member FromSeconds sec  = DateTime.MinValue.AddSeconds(float(sec))

module Map =
  let merge strategy oldMap newMap =
    Map.fold_left (fun acc k v ->
                     let v' = if Map.mem k acc
                                then strategy acc.[k] v
                                else v
                     Map.add k v' acc) oldMap newMap


module String =
  open System.Text.RegularExpressions

  let singular str = Regex.Match(str, "(.*)s$").Groups.[1].Value