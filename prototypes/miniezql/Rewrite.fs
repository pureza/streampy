#light

open Ast
open TypeChecker
open Types
open Extensions
open PatternMatching
open Util


(*
 * Replaces the given expression with another.
 *)
let rec visit expr visitor =
  match expr with
  | FuncCall (fn, param) -> FuncCall (visitor fn, visitor param)
  | MethodCall (target, name, paramExps) -> MethodCall (visitor target, name, List.map visitor paramExps)
  | ArrayIndex (target, index) -> ArrayIndex (visitor target, visitor index)
  | MemberAccess (target, name) -> MemberAccess (visitor target, name)
  | Lambda (ids, expr) -> Lambda (ids, visitor expr)
  | BinaryExpr (op, left, right) -> BinaryExpr (op, visitor left, visitor right)
  | Let (pattern, optType, binder, body) -> Let (visitor pattern, optType, visitor binder, visitor body)
  | LetListener (pattern, optType, binder, listeners, body) ->
      let listeners' = List.map (fun (Listener (evOpt, stream, guardOpt, body)) ->
                                       Listener (evOpt, visitor stream, Option.bind (visitor >> Some) guardOpt, visitor body))
                                listeners
      LetListener (visitor pattern, optType, visitor binder, listeners', visitor body)
  | If (cond, thn, els) -> If (visitor cond, visitor thn, visitor els)
  | Match (expr, cases) -> Match (visitor expr, List.map (fun (MatchCase (pattern, body)) -> MatchCase (pattern, visitor body)) cases)
  | Seq (expr1, expr2) -> Seq (visitor expr1, visitor expr2)
  | Record fields -> Record (List.map (fun (n, e) -> (n, visitor e)) fields)
  | Tuple fields -> Tuple (List.map visitor fields)
  | Id _ | Integer _ | Float _ | String _ | Bool _ | SymbolExpr _ | Fail | Null -> expr


(* Translate listeners into calls to listenN
 *
 * x = 0
 *   | when ev in temp_readings -> x + ev.temperature
 *   | when ev in hum_readings if ev.humidity > 30 -> -1
 *
 *  => x = listenN (0, when (temp_readings, fun ev -> fun def -> def + ev.temperature),
 *                     when (hum_readings, fun ev -> fun def -> if ev.humidity > 30 then -1 else def))
 *)
let rec transListeners stmt =
  let transListener def replaceInBody (Listener (evOpt, stream, guardOpt, body)) =
    let body' = replaceInBody def body
    let body'' = match guardOpt with
                 | None -> body'
                 | Some expr -> If (expr, body', Id (Identifier def))
    let ev = match evOpt with
             | Some (Identifier ev) -> ev
             | None -> "$ev"
    FuncCall(Id (Identifier "when"),
      Tuple [stream; Lambda ([Param (Id (Identifier ev), None)],
                     Lambda ([Param (Id (Identifier def), None)], body''))])

  // Find all LetListener's and replace with appropriate calls to listenN
  let rec replaceLetListeners expr =
    match expr with
    | LetListener (Id (Identifier name), optType, binder, listeners, body) ->
        let whens = List.map (transListener name (fun _ -> id)) listeners
        let binder' = FuncCall(Id (Identifier "listenN"), Tuple (binder::whens))
        Let (Id (Identifier name), optType, binder', replaceLetListeners body)
    | _ -> visit expr replaceLetListeners


  match stmt with
  | Def (name, expr, None) -> Def (name, replaceLetListeners expr, None)
  | Def (Identifier name, expr, Some listeners) ->
      let whens = List.map (transListener name (fun _ -> id)) listeners
      let expr' = FuncCall(Id (Identifier "listenN"), Tuple (expr::whens))
      Def (Identifier name, expr', None)
  | Function (name, parameters, body) -> Function (name, parameters, replaceLetListeners body)
(*  | Entity (Identifier ename, (createFrom, assocs, members)) ->
      let members' =
        List.fold (fun members (Member (Identifier self, Identifier name, expr, listenersOpt) as memb) ->
                     match listenersOpt with
                     | None -> members @ [memb]
                     | Some listeners ->
                         let types' =  types.Add(self, types.[ename])
                         let memberType = TyUnknown (typeOf types' expr)
                         let whens = List.map (transListener name memberType (replacer (Identifier self))) listeners
                         let expr' = FuncCall(Id (Identifier "listenN"), [], expr::whens)
                         members @ [Member (Identifier self, Identifier name, expr', None)])
                  [] members
      Entity (Identifier ename, (createFrom, assocs, members'))
      *)
  | _ -> stmt


(*
 * Translates usages of pattern matching into simpler instructions.
 * For example:
 *
 *   let (a, b) = f () in ... is translated into:
 *
 *   let $x = f () in
 *   let  a = $x.1 in
 *   let  b = $x.2 in
 *   ...
 *
 * match ... with is translated into a different form where only
 * the pattern is used, both to match the pattern and to execute
 * the body. If the match fails, the pattern returns Fail. When it
 * finds Fail, eval should try to execute the next case.
 *
 *   match f () with
 *   | (0, 0) -> B1
 *   | (0, b) -> B2
 *   | _ -> B3
 *
 *   let $x = f ()
 *   match $x with
 *   | if $x.1 = 0
 *       then if $x.2 = 0
 *              then B1
 *              else Fail
 *       else Fail
 *   | ... code for case 2 ...
 *   | ... code for case 3 ...
 *)
let rec transPatterns stmt =
  let replacePattern pattern binder success failure =
    let insns = patternMatch pattern binder

    List.foldBack (fun insn acc ->
                     match insn with
                     | Bind (v, opT, expr) -> Let (Id (Identifier v), opT, expr, acc)
                     | Guard cond -> If (cond, acc, failure))
                   insns success

  let rec replacer expr =
    match expr with
    | Let (Id (Identifier v), optType, binder, body) -> Let (Id (Identifier v), optType, replacer binder, replacer body)
    | Let (pattern, _, binder, body) -> replacePattern pattern binder (replacer body) Fail
    | Match (expr, cases) ->
        let binderId = fresh ()
        let cases' = List.map (fun (MatchCase (pattern, body)) ->
                                 MatchCase (replacePattern pattern (Id (Identifier binderId)) (replacer body) Fail, String "won't come here"))
                              cases
        Let (Id (Identifier binderId), None, expr, Match (Id (Identifier binderId), cases'))
    | Lambda (args, body) ->
        let args', body' =
          List.foldBack (fun (Param (pattern, ty)) (args, body) ->
                           match pattern with
                           | Id (Identifier _) -> Param (pattern, ty)::args, body
                           | _ -> let argId = fresh ()
                                  Param (Id (Identifier argId), None)::args, replacePattern pattern (Id (Identifier argId)) body Fail)
                        args ([], replacer body)
        Lambda (args', body')
    | _ -> visit expr replacer

  match stmt with
  | Def (name, expr, listeners) -> Def (name, replacer expr, listeners)
  | Expr expr -> Expr (replacer expr)
  | DefVariant (name, vs) -> stmt
  | Function (name, parameters, body) ->
      let args', body' =
          List.foldBack (fun (Param (pattern, ty)) (args, body) ->
                           match pattern with
                           | Id (Identifier _) -> Param (pattern, ty)::args, body
                           | _ -> let argId = fresh ()
                                  Param (Id (Identifier argId), None)::args, replacePattern pattern (Id (Identifier argId)) body Fail)
                        parameters ([], replacer body)
      Function (name, args', body')
  | _ -> stmt


(* Replaces Function definitions with equivalent let expressions
 *
 * Example:
 *
 * define x2(n:int) : int = n * 2
 *
 *  => x2 = let x2 = fun (n:int) -> n * 2 in
 *          x2
 *
 * Also creates the initializers for variants:
 *
 * enum State =
 *   | A of int
 *
 *  => define A($1:int) : State = $makeEnum ("A", $1)
 *)
let rec transFunctions stmt =
  match stmt with
  | DefVariant (Identifier name, variants) ->
      let functions = List.fold (fun acc (vname, meta) ->
                                   let parameters = [Param (Id (Identifier "$1"), Some meta)]
                                   let body = FuncCall (Id (Identifier "$makeEnum"), Tuple ([Id vname; Id (Identifier "$1")]))
                                   acc @ [Function (vname, parameters, body)])
                                [] variants
      List.collect transFunctions functions
  | Function (Identifier name, parameters, body) ->
      [Def (Identifier name, Let (Id (Identifier name), None, Lambda (parameters, body), Id (Identifier name)), None)]
  | _ -> [stmt]


let rewrite1 stmts =
  stmts |> List.map transPatterns |> List.map transListeners

let rewrite2 stmts =
  stmts |> List.collect transFunctions