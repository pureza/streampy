﻿#light

open Ast
open TypeChecker

(*
 * Delay window creation as much as possible.
 *
 * Example transformations:
 *   - stream[3 sec].where(...) => stream.where(...)[3 sec]
 *
 *   - x = stream[3 sec]        => x = stream[3 sec]
 *     x.where(...)                stream.where(...)[3 sec]
 *
 * Note that window creation occurs always at the end of the line.
 * This is not always possible: for example, join receives at least
 * one window and results in a second window.
 *)



type ExprsContext = Map<string, expr>

let rec lookupExpr var (varExprs:ExprsContext) =
  match varExprs.[var] with
  | Id (Identifier var') -> lookupExpr var' varExprs
  | other -> other

let rec delayWindows (varExprs:ExprsContext) (types:TypeContext) = function
  | Def (Identifier name, expr) ->
      let expr' = delayWindowsExpr varExprs types expr
      varExprs.Add(name, expr'), Def (Identifier name, expr')
  | Expr expr -> varExprs, Expr (delayWindowsExpr varExprs types expr)
  | Entity (name, ((source, uniqueId), assocs, attributes)) as expr ->
      varExprs, Entity (name, ((delayWindowsExpr varExprs types source, uniqueId), assocs, attributes))

and delayWindowsExpr varExprs types expr =   
  let rec splitArrayIndex expr =
    match expr with
    | ArrayIndex(target, index) -> target, index
    | Id (Identifier var) -> splitArrayIndex (lookupExpr var varExprs) 
    | _ -> failwith "expr is not a window"

  match expr with
  | MethodCall (ArrayIndex (target, index), (Identifier name), paramExps) ->
      // The most simple case: reorder the operations so that the window
      // creation happens later
      let targetType = typeOf types target
      match targetType with
      | TyStream _ -> 
          match name with
          | "where" -> ArrayIndex(MethodCall (target, (Identifier name), paramExps), index)
          | _ -> expr
      | _ -> expr
  | MethodCall (Id (Identifier var), (Identifier name), paramExps) ->
      // Replace the var's id with its defining expression and recurse
      match types.[var] with
      | TyWindow (TyStream _, TimedWindow _) ->
          match name with
          | "where" -> delayWindowsExpr varExprs types (MethodCall (lookupExpr var varExprs, (Identifier name), paramExps))
          | _ -> expr
      | _ -> expr
  | MethodCall (target, (Identifier name), paramExps) ->
      // General case: delay the target and delay the entire expression if needed.
      let target' = delayWindowsExpr varExprs types target
      let expr' = MethodCall (target', (Identifier name), paramExps)
      if target' <> target
        then delayWindowsExpr varExprs types expr'
        else expr'
  // Take this out of here!
  | RecordWith (Id (Identifier name), newFields) ->
      match varExprs.[name] with
      | Record fields -> Record (List.fold_left (fun fields (sym, expr) -> fields @ [sym, expr]) fields newFields)
      | _ -> failwithf "RecordWith: The source is not a record!"
  | RecordWith (source, newFields) ->
      match delayWindowsExpr varExprs types source with
      | Record fields -> Record (List.fold_left (fun fields (sym, expr) -> fields @ [sym, expr]) fields newFields)
      | _ -> failwithf "RecordWith: The source is not a record!"
  // BinOps in continuous value windows won't be allowed, so this will be removed eventually.        
        (*
  | BinaryExpr (oper, expr1, expr2) ->
    // Delay both subexpressions and then delay the entire expression if needed
    let expr1' = delayWindowsExpr varExprs types expr1
    let expr2' = delayWindowsExpr varExprs types expr2
    
    match expr1', expr2' with
    | Id (Identifier name1), _ -> delayWindowsExpr varExprs types (BinaryExpr (oper, lookupExpr name1 varExprs, expr2'))
    | _, Id (Identifier name2) -> delayWindowsExpr varExprs types (BinaryExpr (oper, expr1', lookupExpr name2 varExprs))
    | ArrayIndex (source1, index1), ArrayIndex (source2, index2) -> ArrayIndex (BinaryExpr (oper, source1, source2), index1)
    | ArrayIndex (source1, index1), _ -> ArrayIndex (BinaryExpr (oper, source1, expr2'), index1)
    | _, ArrayIndex (source2, index2) -> ArrayIndex (BinaryExpr (oper, expr1', source2), index2)
    | _, _ -> BinaryExpr (oper, expr1', expr2')
    *)
  | _ -> expr


(*
 * Rewrites entity declarations into groupby operations
 *)

let rec replace expr replacer =
  match expr with
  | MethodCall (target, name, paramExps) -> MethodCall (replacer target, name, List.map replacer paramExps)
  | FuncCall (fn, paramExps) -> FuncCall (replacer fn, List.map replacer paramExps)
  | MemberAccess (target, name) -> MemberAccess (replacer target, name)
  | ArrayIndex(target, index) -> ArrayIndex (replacer target, replacer index)
  | Lambda (ids, expr) -> Lambda (ids, replacer expr) 
  | BinaryExpr (op, left, right) -> BinaryExpr (op, replacer left, replacer right)
  | Let (Identifier name, binder, body) -> Let (Identifier name, replacer binder, replacer body)
  | If (cond, thn, els) -> If (replacer cond, replacer thn, replacer els)
  | Seq (expr1, expr2) -> Seq (replacer expr1, replacer expr2)
  | Record fields -> Record (List.map (fun (n, e) -> (n, replacer e)) fields)
  | RecordWith (source, newFields) -> RecordWith (replacer source, List.map (fun (n, e) -> (n, replacer e)) newFields)
  | Time (length, unit) -> Time (replacer length, unit)
  | Id _ | Integer _ | String _ | Bool _ | SymbolExpr _ -> expr
  | _ -> failwithf "not implemented: %A" expr

let rec translateEntities (entities:Set<string>) (types:TypeContext) = function
  | Def (Identifier name, expr) -> entities, Def (Identifier name, translateEntitiesExpr entities types expr)
  | Expr expr -> entities, Expr (translateEntitiesExpr entities types expr)
  | Entity (Identifier name, ((source, uniqueId), assocs, members)) as expr ->
      let streamFields = match typeOf types source with
                         | TyStream f -> f
                         | _ -> failwith "can't happen!"
      let streamFields = Map.of_list [ for f in streamFields ->
                                         (Symbol f, MethodCall (Id (Identifier "g"), Identifier "last",
                                                                [SymbolExpr (Symbol f)]))]
                                                                
      let assocFields = List.fold_left (fun (acc:Map<symbol, expr>) assoc ->
                                        match assoc with
                                        | BelongsTo (Symbol entity) -> 
                                            let entityIdExpr = acc.[Symbol (entity + "_id")]
                                            let fieldExpr = ArrayIndex (Id (Identifier ("$" + (String.capitalize entity) + "_all")), entityIdExpr)
                                            acc.Add(Symbol entity, fieldExpr))
                                      streamFields assocs
                                      
      let allFields = List.fold_left (fun acc (Member (self, Identifier name, expr)) ->
                                        let expr' = translateSelf acc expr
                                        acc.Add(Symbol name, expr'))
                                      assocFields members

      let allDictName = "$" + name + "_all"
      let record = Record (Map.to_list allFields)
      let groupByExpr = MethodCall(source, Identifier "groupby",
                                   [SymbolExpr uniqueId; Lambda ([Identifier "g"], record)])
      printfn "%A" groupByExpr
      let assign = Def (Identifier allDictName, groupByExpr)                 
      entities.Add(name), assign

and translateEntitiesExpr entities types expr =
  let rec replacer expr = match expr with
                          | MemberAccess (Id (Identifier target), Identifier "all") when Set.mem target entities -> (Id (Identifier ("$" + target + "_all")))
                          | _ -> replace expr replacer
  let expr' = replacer expr
  if expr' <> expr then expr' else replace expr replacer

and translateSelf (fieldExprs:Map<symbol, expr>) expr =
  let rec replacer expr = match expr with
                          | MemberAccess (Id (Identifier "self"), Identifier field) -> fieldExprs.[Symbol field]
                          | _ -> replace expr replacer
                            
  let expr' = replacer expr
  if expr' <> expr then expr' else replace expr replacer


let rewrite types ast =

  let varExprs, stmts' =
    List.fold_left (fun (varExprs, stmts) stmt ->
                      let varExprs', stmt' = delayWindows varExprs types stmt
                      varExprs', stmts @ [stmt'])
                   (Map.empty, []) ast
                              
  let entities, stmts'' =
    List.fold_left (fun (entities, stmts) stmt ->
                      let entities, stmt' = translateEntities entities types stmt
                      entities, stmts @ [stmt'])
                   (Set.empty, []) stmts'
                   
  stmts''
  
      