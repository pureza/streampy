module DateTimeExtensions =
    open System

    type DateTime with
        member self.TotalSeconds = (int (TimeSpan(self.Ticks).TotalSeconds))
        static member FromSeconds sec  = DateTime.MinValue.AddSeconds(float(sec))

module Map =
  let keySet map = map |> Map.to_list |> List.map fst |> Set.of_list

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


module List =
  
  (* Longest common and __continuous__ subsequence between two lists *)
  let rec lccs list1 list2 =
    (* Returns a pair of lists: the first element contains the first common subsequence
     * ahead (that may be incomplete because previous elements may belong to it) and
     * the second contains the the longest common and continuous subsequence after
     * the first 
     * It also returns the position where the subseq starts in both lists *)
    let rec helper list1 list2 n1 n2 = 
      match list1, list2 with
      | [], _ | _, [] -> ([], -1, -1), ([], -1, -1)
      | x::xs, y::ys when x = y ->
          match helper xs ys n1 n2 with
          | (this, _, _), rest -> (x::this, n1, n2), rest
      | x::xs, y::ys ->
          // Here we know that the previous subsequence has been broken (x <> y).
          // Thus, first1 and first2 will be complete.
          let first1, rest1 = helper list1 ys n1 (n2 + 1)
          let first2, rest2 = helper xs list2 (n1 + 1) n2
          let greatestAhead = List.maxBy (fun (x, _, _) -> List.length x) [first1; rest1; first2; rest2]
          ([], -1, -1), greatestAhead // [] means that the previous subsequence finished.
    
    let (first, n11, n12), (ahead, n21, n22) = helper list1 list2 0 0
    if first.Length >= ahead.Length then (first, n11, n12) else (ahead, n21, n22)

    
  let rec remove elt list =
    match list with
    | x::xs when x = elt -> remove elt xs
    | x::xs -> x::(remove elt xs)
    | [] -> []

  let rec removeFirst elt list =
    match list with
    | x::xs when x = elt -> xs
    | x::xs -> x::(remove elt xs)
    | [] -> []    


module String =
  open System.Text.RegularExpressions

  let singular str = Regex.Match(str, "(.*)s$").Groups.[1].Value