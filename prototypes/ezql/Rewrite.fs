#light

open Ast
open TypeChecker
open Extensions
open Util

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
  | Let (Identifier name, optType, binder, body) -> Let (Identifier name, optType, visitor binder, visitor body)
  | If (cond, thn, els) -> If (visitor cond, visitor thn, visitor els)
  | Match (expr, cases) -> Match (visitor expr, List.map (fun (MatchCase (label, meta, body)) -> MatchCase (label, meta, visitor body)) cases)
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
  | DefVariant _ as expr -> varExprs, expr
  | Def (Identifier name, expr, None) ->
      let expr' = delayWindowsExpr varExprs types expr
      varExprs.Add(name, expr'), Def (Identifier name, expr', None)
  | Expr expr -> varExprs, Expr (delayWindowsExpr varExprs types expr)
  | Entity (name, ((source, uniqueId), assocs, attributes)) as expr ->
      varExprs, Entity (name, ((delayWindowsExpr varExprs types source, uniqueId), assocs, attributes))
  | _ -> failwithf "Won't happen because function translations happen first."

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
 *                                         temperature = g.last(:temperature) }  
 *
 * $Product_all = entries
 *                  .groupby(:product_id, g -> let room_id = g.last(:room_id) in
 *                                             let room = Room.all[room_id] in
 *                                             { :product_id  = g.last(:product_id),
 *                                               :room_id     = room_id,
 *                                               :room        = $ref(room),
 *                                               temperature = room.temperature })
 *
 * (The "let's" are shown here only to improve readability, they are not
 * generated by the algorithm)
 *
 * "entities" collects the names of declared entities.
 *)
let rec transEntities (entities:Set<string>) (types:TypeContext) = function
  | DefVariant _ as expr -> entities, expr
  | Def (Identifier name, expr, None) -> entities, Def (Identifier name, transDictAll entities expr, None)
  | Expr expr -> entities, Expr (transDictAll entities expr)
  | Entity (Identifier name, ((source, uniqueId), assocs, members)) as expr ->
      let streamFields = match typeOf types source with
                         | TyStream (TyRecord f) -> f
                         | _ -> failwith "can't happen!"
      // Translate the fields inherited from the stream                         
      let streamFields = Map.of_list [ for pair in streamFields ->
                                         (pair.Key, MethodCall (Id (Identifier "g"), Identifier "last",
                                                                 [SymbolExpr (Symbol pair.Key)]))]
      // Translate associations                                                                
      let assocFields = List.fold (fun (acc:Map<string, expr>) assoc ->
                                     match assoc with
                                     | BelongsTo (Symbol entity) -> 
                                         let entityIdExpr = acc.[entity + "_id"]
                                         let indexEntity = ArrayIndex (Id (Identifier (entityDict (String.capitalize entity))), entityIdExpr)
                                         let fieldExpr = FuncCall(Id (Identifier "$ref"), [indexEntity])
                                         acc.Add(entity, fieldExpr)
                                     | HasMany (Symbol entity) ->
                                         let entityName = String.capitalize entity |> String.singular
                                         let entityId = (name + "_id").ToLower()
                                         let x = Identifier "x"
                                         let filter = Lambda ([Param (x, None)], BinaryExpr (Equal, MemberAccess (Id x, Identifier entityId), acc.[entityId]))
                                         let whereExpr = MethodCall (Id (Identifier (entityDict entityName)), Identifier "where", [filter])
                                         let toRef = Lambda ([Param (x, None)], FuncCall (Id (Identifier "$ref"), [Id x]))
                                         let selectRef = MethodCall (whereExpr, Identifier "select", [toRef])
                                         acc.Add(entity, selectRef))
                                   streamFields assocs
      // Translate additional member declarations.                                      
      let allFields = List.fold (fun acc (Member (self, Identifier name, expr, listeners)) ->
                                   let expr' = transDictAll entities expr
                                   let expr'' = transSelf acc expr' self
                                   acc.Add(name, expr''))
                                assocFields members

      let record = Record (Map.to_list allFields)
      let groupByExpr = MethodCall(source, Identifier "groupby",
                                   [SymbolExpr uniqueId; Lambda ([Param (Identifier "g", None)], record)])
      let assign = Def (Identifier (entityDict name), groupByExpr, None)                 
      entities.Add(name), assign
  | _ -> failwithf "Won't happen because function translations happen first."

(* Replaces Entity.all with $Entity_all, everywhere *)
and transDictAll entities expr =
  let rec replacer expr = match expr with
                          | MemberAccess (Id (Identifier target), Identifier "all") when Set.contains target entities -> (Id (Identifier (entityDict target)))
                          | _ -> visit expr replacer
  replacer expr

(* Replaces self.field with the expression that originates field. *)
and transSelf (fieldExprs:Map<string, expr>) expr self =
  let rec replacer expr = match expr with
                          | MemberAccess (Id self', Identifier field) when self = self' -> fieldExprs.[field]
                          | Let (self', _, _, _) when self = self' -> expr
                          | Lambda (args, _) when List.exists (fun (Param (self', _)) -> self = self') args -> expr
                          | _ -> visit expr replacer

  replacer expr                            

(* Replaces RecordWith expressions with equivalent Record expressions *)     
let rec transRecordWith (varExprs:Map<string, expr>) stmt =
  let rec replacer expr = match expr with
                          | RecordWith (Id (Identifier name), newFields) ->
                              match lookupExpr name varExprs with
                              | Record fields -> Record (List.fold (fun fields (sym, expr) -> fields @ [sym, replacer expr]) fields newFields)
                              | _ -> failwithf "RecordWith: The source is not a record!"
                          | RecordWith (source, newFields) ->
                              match replacer source with
                              | Record fields -> Record (List.fold (fun fields (sym, expr) -> fields @ [sym, replacer expr]) fields newFields)
                              | _ -> failwithf "RecordWith: The source is not a record!"
                          | _ -> visit expr replacer

  match stmt with
  | DefVariant _ as expr ->varExprs, expr
  | Def (Identifier name, expr, None) ->
      let expr' = replacer expr
      varExprs.Add(name, expr'), Def (Identifier name, expr', None)
  | Expr expr -> varExprs, Expr (replacer expr)
  | _ -> failwithf "Won't happen because entity and function translations happens first."


(* Replaces Function definitions with equivalent let expressions *)     
let rec transFunctions (types:TypeContext) stmt =
  match stmt with
  | DefVariant (Identifier name, variants) ->
      let functions = List.fold (fun acc (vname, meta) ->
                                   let parameters = [Param (Identifier "$1", Some meta)]
                                   let body = FuncCall (Id (Identifier "$makeEnum"), [Id vname; Id (Identifier "$1")])
                                   acc @ [Function (vname, parameters, types.[name], body)]) [] variants
      List.collect (transFunctions types) functions
  | Function (Identifier name, parameters, retType, body) ->
      let fnType = types.[name]
      [Def (Identifier name, Let (Identifier name, Some fnType, Lambda (parameters, body), Id (Identifier name)), None)]
  | _ -> [stmt]

(* Translate listeners into calls to listenN *)
let rec transListeners types stmt =
  let rec replacer self field expr =
    match expr with
    | MemberAccess (Id self', Identifier field') when field = field' && self' = self -> Id (Identifier field)
    | Let (self', _, _, _) when self = self' -> expr
    | Lambda (args, _) when List.exists (fun (Param (self', _)) -> self = self') args -> expr
    | _ -> visit expr (replacer self field)

  let transListener def defType replaceInBody (Listener (evOpt, streamOpt, guardOpt, body)) =
    let body' = replaceInBody def body
    let body'' = match guardOpt with
                 | None -> body'
                 | Some expr -> If (expr, body', Id (Identifier def))
    let ev = match evOpt with
             | Some (Identifier ev) -> ev
             | None -> "$ev"
    let stream = match streamOpt, guardOpt with
                 | Some stream, _ -> stream
                 | _, Some guard -> MethodCall (guard, Identifier "updated", [])
                 | _ , _ -> failwith "Can't happen"
    FuncCall(Id (Identifier "when"),
      [stream; Lambda ([Param (Identifier ev, None)],
                 Lambda ([Param (Identifier def, Some defType)], body''))])
  
  match stmt with
  | Def (Identifier name, expr, Some listeners) ->
      let defType = typeOf types expr
      let whens = List.map (transListener name defType (fun _ -> id)) listeners
      let expr' = FuncCall(Id (Identifier "listenN"), expr::whens)
      Def (Identifier name, expr', None)
  | Entity (Identifier ename, (createFrom, assocs, members)) ->
      let members' =
        List.fold (fun members (Member (Identifier self, Identifier name, expr, listenersOpt) as memb) ->
                     match listenersOpt with
                     | None -> members @ [memb]
                     | Some listeners ->
                         let types' = types.Add(self, TyEntity ename)
                         let memberType = typeOf types' expr
                         let whens = List.map (transListener name memberType (replacer (Identifier self))) listeners
                         let expr' = FuncCall(Id (Identifier "listenN"), expr::whens)
                         members @ [Member (Identifier self, Identifier name, expr', None)])
                  [] members
      Entity (Identifier ename, (createFrom, assocs, members'))
  | _ -> stmt

let rewrite types ast =
  let stmts1 =
    List.fold (fun stmts stmt ->
                 let stmt' = transFunctions types stmt
                 stmts @ stmt')
              [] ast
              
  let stmts2 =
    List.fold (fun stmts stmt ->
                 let stmt' = transListeners types stmt
                 stmts @ [stmt'])
              [] stmts1              

  let _, stmts3 =
    List.fold (fun (varExprs, stmts) stmt ->
                 let varExprs', stmt' = delayWindows varExprs types stmt
                 varExprs', stmts @ [stmt'])
              (Map.empty, []) stmts2
                        
  let _, stmts4 =
    List.fold (fun (entities, stmts) stmt ->
                 let entities, stmt' = transEntities entities types stmt
                 entities, stmts @ [stmt'])
              (Set.empty, []) stmts3

  let _, stmts5 =
    List.fold (fun (entities, stmts) stmt ->
                 let entities, stmt' = transRecordWith entities stmt
                 entities, stmts @ [stmt'])
              (Map.empty, []) stmts4
                   
  stmts5
  
      