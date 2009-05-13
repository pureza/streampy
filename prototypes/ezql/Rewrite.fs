﻿#light

open Ast
open TypeChecker
open Extensions

(*
 * Replaces the given expression with another.
 *)
let rec visit expr visitor =
  match expr with
  | MethodCall (target, name, paramExps) -> MethodCall (visitor target, name, List.map visitor paramExps)
  | FuncCall (fn, paramExps) -> FuncCall (visitor fn, List.map visitor paramExps)
  | MemberAccess (target, name) -> MemberAccess (visitor target, name)
  | ArrayIndex(target, index) -> ArrayIndex (visitor target, visitor index)
  | Lambda (ids, expr) -> Lambda (ids, visitor expr) 
  | BinaryExpr (op, left, right) -> BinaryExpr (op, visitor left, visitor right)
  | Let (Identifier name, binder, body) -> Let (Identifier name, visitor binder, visitor body)
  | If (cond, thn, els) -> If (visitor cond, visitor thn, visitor els)
  | Seq (expr1, expr2) -> Seq (visitor expr1, visitor expr2)
  | Record fields -> Record (List.map (fun (n, e) -> (n, visitor e)) fields)
  | RecordWith (source, newFields) -> RecordWith (visitor source, List.map (fun (n, e) -> (n, visitor e)) newFields)
  | Time (length, unit) -> Time (visitor length, unit)
  | Id _ | Integer _ | String _ | Bool _ | SymbolExpr _ -> expr

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


let entityDict entity = sprintf "$%s_all" entity

(*
 * Rewrites entity declarations into groupby operations.
 * 
 * Example:
 *
 * entity Room =
 *   createFrom(temp_readings, :room_id)
 *
 * entity Product =
 *   createFrom (entries, :product_id)
 *   belongsTo :room
 *   member self.temperature = self.room.temperature;;
 *
 * gets translated into:
 *
 * $Room_all = temp_readings
 *               .groupby(:room_id, g -> { :room_id     = g.last(:room_id),
 *                                         :temperature = g.last(:temperature) }  
 *
 * $Product_all = entries
 *                  .groupby(:product_id, g -> let room_id = g.last(:room_id) in
 *                                             let room = Room.all[room_id] in
 *                                             { :product_id  = g.last(:product_id),
 *                                               :room_id     = room_id,
 *                                               :room        = room,
 *                                               :temperature = room.temperature })
 *
 * (The "let's" are shown here only to improve readability, they are not
 * generated by the algorithm)
 *
 * "entities" collects the names of declared entities.
 *)
let rec transEntities (entities:Set<string>) (types:TypeContext) = function
  | Def (Identifier name, expr) -> entities, Def (Identifier name, transDictAll entities expr)
  | Expr expr -> entities, Expr (transDictAll entities expr)
  | Entity (Identifier name, ((source, uniqueId), assocs, members)) as expr ->
      let streamFields = match typeOf types source with
                         | TyStream (TyRecord f) -> f
                         | _ -> failwith "can't happen!"
      // Translate the fields inherited from the stream                         
      let streamFields = Map.of_list [ for pair in streamFields ->
                                         (Symbol pair.Key, MethodCall (Id (Identifier "g"), Identifier "last",
                                                                      [SymbolExpr (Symbol pair.Key)]))]
      // Translate associations                                                                
      let assocFields = List.fold_left (fun (acc:Map<symbol, expr>) assoc ->
                                        match assoc with
                                        | BelongsTo (Symbol entity) -> 
                                            let entityIdExpr = acc.[Symbol (entity + "_id")]
                                            let fieldExpr = ArrayIndex (Id (Identifier (entityDict (String.capitalize entity))), entityIdExpr)
                                            acc.Add(Symbol entity, fieldExpr)
                                        | HasMany (Symbol entity) ->
                                            let entityName = String.capitalize entity |> String.singular
                                            let entityId = String.lowercase (name + "_id")
                                            let x = Identifier "x"
                                            let filter = Lambda ([x], BinaryExpr (Equal, MemberAccess (Id x, Identifier entityId), acc.[Symbol entityId]))
                                            let fieldExpr = MethodCall (Id (Identifier (entityDict entityName)), Identifier "where", [filter])
                                            acc.Add(Symbol entity, fieldExpr))
                                      streamFields assocs
      // Translate additional member declarations.                                      
      let allFields = List.fold_left (fun acc (Member (self, Identifier name, expr)) ->
                                        let expr' = transSelf acc expr name
                                        acc.Add(Symbol name, expr'))
                                      assocFields members

      let record = Record (Map.to_list allFields)
      let groupByExpr = MethodCall(source, Identifier "groupby",
                                   [SymbolExpr uniqueId; Lambda ([Identifier "g"], record)])
      let assign = Def (Identifier (entityDict name), groupByExpr)                 
      entities.Add(name), assign

(* Replaces Entity.all with $Entity_all, everywhere *)
and transDictAll entities expr =
  let rec replacer expr = match expr with
                          | MemberAccess (Id (Identifier target), Identifier "all") when Set.mem target entities -> (Id (Identifier (entityDict target)))
                          | _ -> visit expr replacer
  replacer expr

(* Replaces self.field with the expression that originates field. *)
and transSelf (fieldExprs:Map<symbol, expr>) expr self =
  let rec replacer expr = match expr with
                          | MemberAccess (Id (Identifier self), Identifier field) -> fieldExprs.[Symbol field]
                          | _ -> visit expr replacer

  replacer expr                            

(* Replaces RecordWith expressions with equivalent Record expressions *)     
let rec transRecordWith (varExprs:Map<string, expr>) stmt =
  let rec replacer expr = match expr with
                          | RecordWith (Id (Identifier name), newFields) ->
                              match lookupExpr name varExprs with
                              | Record fields -> Record (List.fold_left (fun fields (sym, expr) -> fields @ [sym, replacer expr]) fields newFields)
                              | _ -> failwithf "RecordWith: The source is not a record!"
                          | RecordWith (source, newFields) ->
                              match replacer source with
                              | Record fields -> Record (List.fold_left (fun fields (sym, expr) -> fields @ [sym, replacer expr]) fields newFields)
                              | _ -> failwithf "RecordWith: The source is not a record!"
                          | _ -> visit expr replacer

  match stmt with
  | Def (Identifier name, expr) ->
      let expr' = replacer expr
      varExprs.Add(name, expr'), Def (Identifier name, expr')
  | Expr expr -> varExprs, Expr (replacer expr)
  | _ -> failwithf "Won't happen because entity translation happens first."


let rewrite types ast =
  let _, stmts1 =
    List.fold_left (fun (varExprs, stmts) stmt ->
                      let varExprs', stmt' = delayWindows varExprs types stmt
                      varExprs', stmts @ [stmt'])
                   (Map.empty, []) ast
                              
  let _, stmts2 =
    List.fold_left (fun (entities, stmts) stmt ->
                      let entities, stmt' = transEntities entities types stmt
                      entities, stmts @ [stmt'])
                   (Set.empty, []) stmts1

  let _, stmts3 =
    List.fold_left (fun (entities, stmts) stmt ->
                      let entities, stmt' = transRecordWith entities stmt
                      entities, stmts @ [stmt'])
                   (Map.empty, []) stmts2
                   
  stmts3
  
      