module DateTimeExtensions =
    open System

    type DateTime with
        member self.TotalSeconds = (int (TimeSpan(self.Ticks).TotalSeconds))
        static member FromSeconds sec  = DateTime.MinValue.AddSeconds(float(sec))

module Map =
  let appendToList key value map =
    let v' = if Map.contains key map
               then map.[key] @ [value]
               else [value]
    Map.add key v' map

  let merge strategy oldMap newMap =
    Map.fold_left (fun acc k v ->
                     let v' = if Map.contains k acc
                                then strategy acc.[k] v
                                else v
                     Map.add k v' acc) oldMap newMap
                     
  let union a b = merge (fun va vb -> vb) a b


module String =
  open System.Text.RegularExpressions

  let singular str = Regex.Match(str, "(.*)s$").Groups.[1].Value