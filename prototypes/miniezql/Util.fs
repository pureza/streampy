open Extensions
open Types
open Ast

let first fn (a, b) = (fn a, b)

let isTimeLength = function
  | MemberAccess (length, Identifier ("sec" | "min" | "hour" | "day")) -> true
  | _ -> false

(*
 * Create unique global variables.
 *)
let fresh =
  let counter = ref 0
  fun () ->
    let label = !counter
    counter := !counter + 1
    sprintf "$%d" label