#light

open Extensions.DateTimeExtensions
open Extensions
open Ast
open Types
open OperUtil

exception MatchFail of string

let rec eval (env:Context) = function
  | ArrayIndex (target, index) ->
      let tv = eval env target
      let iv = eval env index
      match tv with
      | VDict dict -> if Map.contains iv dict then dict.[iv] else VNull //failwithf "Dictionary doesn't contain key %A. env = %A" iv env
      | VWindow contents ->
          match iv with
          | VInt i when i < 0 ->
              let i' = i + contents.Length
              if i' >= 0 && i' < contents.Length then contents.[i'] else VNull
          | VInt i -> if i < contents.Length then contents.[i] else VNull
          | _ -> failwithf "The index is not an integer, but a %A" iv
      | _ -> failwithf "[]: Not a dictionary"
  | MemberAccess (expr, Identifier "null?") ->
      match eval env expr with
      | VNull -> VBool true
      | _ -> VBool false
  | MemberAccess (target, Identifier field) ->
      match eval env target with
      | VRecord fields -> fields.[VString field]
      | VTuple elts -> elts.[int field - 1]
      | VNull -> VNull
      | other -> failwithf "Not a record: %A" other
  | FuncCall (expr, param) ->
      match expr with
      | Id (Identifier name) when Map.contains name env && (env.[name]).IsPrimitive() ->
          let handler = match env.[name] with
                        | VPrimitive handler -> handler
                        | _ -> failwithf "Can't happen"
          handler eval env param
      | _ -> let fn = eval env expr
             let pv = eval env param
             apply fn pv
  | Record fields ->
      // This is extremely ineficient - we should create a node if possible
      VRecord (fields |> List.map (fun (name, expr) -> (VString name, eval env expr))
                      |> Map.of_list)
  | Tuple fields -> VTuple (List.map (eval env) fields)
  | Lambda (args, body) as fn -> VClosure (env, fn, None)
  | Let (Id (Identifier name), _, (Lambda _ as fn), body) ->
      eval (env.Add(name, VClosure (env, fn, Some name))) body
  | Let (Id (Identifier name), _, binder, body) ->
      eval (env.Add(name, eval env binder)) body
  | Let _ -> failwithf "eval: The bounded variable is a pattern. Rewrite was supposed to eliminate this."
  | If (cond, thn, els) ->
      match eval env cond with
      | VNull -> VNull
      | VBool true -> eval env thn
      | other -> eval env els
  | Match (Id (Identifier _), cases) ->
      let result = List.tryPick (fun (MatchCase (pattern, _)) ->
                                   try
                                     Some (eval env pattern)
                                   with MatchFail _ -> None) cases
      match result with
      | Some v -> v
      | _ -> failwithf "Incomplete matches in match ... with expression"
  | BinaryExpr (oper, expr1, expr2) ->
    let value1 = eval env expr1
    let value2 = eval env expr2
    evalOp (oper, value1, value2)
  | Seq (expr1, expr2) ->
      eval env expr1 |> ignore
      eval env expr2
  | Id (Identifier name) ->
      match Map.tryFind name env with
      | Some v -> v
      | _ -> failwithf "eval: Unknown variable or identifier: %s" name
  | Integer v -> VInt v
  | Float f -> value.VFloat f
  | String v -> VString v
  | Bool v -> VBool v
  | SymbolExpr (Symbol s) -> VSymbol s
  | Fail -> raise (MatchFail "Match failure.")
  | Null -> VNull
  | other -> failwithf "Unknown expression: %A. Maybe rewrite was supposed to eliminate this?" other


and evalOp = function
  | Plus,               v1, v2 -> value.Add(v1, v2)
  | Minus,              v1, v2 -> value.Subtract(v1, v2)
  | Times,              v1, v2 -> value.Multiply(v1, v2)
  | Div,                v1, v2 -> value.Div(v1, v2)
  | Mod,                v1, v2 -> value.Mod(v1, v2)
  | GreaterThan,        v1, v2 -> value.GreaterThan(v1, v2)
  | GreaterThanOrEqual, v1, v2 -> value.GreaterThanOrEqual(v1, v2)
  | LessThan,           v1, v2 -> value.LessThan(v1, v2)
  | LessThanOrEqual,    v1, v2 -> value.LessThanOrEqual(v1, v2)
  | Equal,              v1, v2 -> value.Equals(v1, v2)
  | NotEqual,           v1, v2 -> value.Differ(v1, v2)
  | And,                v1, v2 -> value.And(v1, v2)
  | Or,                 v1, v2 -> value.Or(v1, v2)
  | Is,                 v1, v2 -> value.Is(v1, v2)

and apply value =
  match value with
  | VClosure (env', expr, itself) ->
      let primitives = Map.of_list [ for f in Primitives.primitiveFunctions -> (f.Name, VPrimitive f.Eval) ]
      // If the closure is recursive, add itself to the environment
      let env = Map.union (match itself with
                           | Some name -> env'.Add(name, value)
                           | _ -> env')
                          primitives
      match expr with
      | Lambda (ids, body) ->
          (fun arg ->
              let name = match ids.[0] with
                         | Param (Id (Identifier name), _) -> name
                         | _ -> failwithf "The parameter to the lambda is a complex pattern. Rewrite was supposed to eliminate this."
              let env' = env.Add(name, arg)
              let curry = match ids with
                          | x::y::xs -> Lambda (y::xs, body)
                          | x::[] -> body
                          | [] -> failwithf "Too much parameters!"
              eval env' curry)
      | _ -> failwith "evalClosure: Wrong type"
  | VClosureSpecial (_, expr, _, _, itself) -> apply (convertClosureSpecial value) // FIXME: Ugly stuff is ugly
  | _ -> failwith "This is not a closure"
