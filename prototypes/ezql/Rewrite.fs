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
  | FixedAccess expr -> FixedAccess (visitor expr)
  | ArrayIndex (target, index) -> ArrayIndex (visitor target, visitor index)
  | Lambda (ids, expr) -> Lambda (ids, visitor expr) 
  | BinaryExpr (op, left, right) -> BinaryExpr (op, visitor left, visitor right)
  | Let (Identifier name, optType, binder, body) -> Let (Identifier name, optType, visitor binder, visitor body)
  | If (cond, thn, els) -> If (visitor cond, visitor thn, visitor els)
  | Match (expr, cases) -> Match (visitor expr, List.map (fun (MatchCase (label, meta, body)) -> MatchCase (label, meta, visitor body)) cases)
  | Seq (expr1, expr2) -> Seq (visitor expr1, visitor expr2)
  | Record fields -> Record (List.map (fun (n, e) -> (n, visitor e)) fields)
  | RecordWith (source, newFields) -> RecordWith (visitor source, List.map (fun (n, e) -> (n, visitor e)) newFields)
  | Time (length, unit) -> Time (visitor length, unit)
  | Id _ | Integer _ | String _ | Bool _ | SymbolExpr _ | Null -> expr
  
  
let rec visitWithTypes (types:TypeContext) expr visitor =
  match expr with
  | MethodCall (target, Identifier name, paramExps) ->
      let paramExps' = match name, paramExps with
                       | "groupby", [field; Lambda ([Param (Identifier arg, None)], body)] ->
                           [field; Lambda ([Param (Identifier arg, Some (typeOf types target))], body)]
                       | ("where" | "select"), [Lambda ([Param (Identifier arg, None)], body)] ->
                           match typeOf types target with
                           | (TyStream (TyRecord _ as argType) | TyWindow (TyStream (TyRecord _ as argType), _) | TyDict argType) ->
                             [Lambda ([Param (Identifier arg, Some argType)], body)]
                           | _ -> failwithf "Can't happen"
                       | ("any?" | "all?"), [Lambda ([Param (Identifier arg, None)], body)] ->
                           match typeOf types target with
                           | (TyStream v | TyWindow (TyStream v, _) | TyWindow (v, _)) -> [Lambda ([Param (Identifier arg, Some v)], body)]
                           | _ -> failwithf "Can't happen"
                       | _ -> paramExps
                        
      MethodCall (visitor types target, Identifier name, List.map (visitor types) paramExps')
  | FuncCall (fn, paramExps) ->
      let paramExps' = match fn, paramExps with
                       | Id (Identifier "when"), [target; Lambda ([Param (Identifier arg, None)], body)] ->
                           match typeOf types target with
                           | TyStream evType -> [target; Lambda ([Param (Identifier arg, Some evType)], body)]
                           | _ -> failwithf "Can't happen"
                       | _ -> paramExps
      FuncCall (visitor types fn, List.map (visitor types) paramExps')
  | MemberAccess (target, name) -> MemberAccess (visitor types target, name)
  | FixedAccess expr -> FixedAccess (visitor types expr)
  | ArrayIndex (target, index) -> ArrayIndex (visitor types target, visitor types index)
  | Lambda (ids, expr) ->
      let types' = List.fold (fun (acc:TypeContext) (Param (Identifier arg, optType)) ->
                                match optType with
                                | _ when arg = "_" -> acc
                                | Some typ -> acc.Add(arg, typ)
                                | None -> failwithf "visitWithTypes: Can't determine the type of %A" arg)
                             types ids
      Lambda (ids, visitor types' expr) 
  | BinaryExpr (op, left, right) -> BinaryExpr (op, visitor types left, visitor types right)
  | Let (Identifier name, optType, binder, body) ->
      let binderType = match optType with
                       | Some typ -> typ
                       | None -> typeOf types binder
      Let (Identifier name, optType, visitor types binder, visitor (types.Add(name, binderType)) body)
  | If (cond, thn, els) -> If (visitor types cond, visitor types thn, visitor types els)
  | Match (expr, cases) ->
      Match (visitor types expr, List.map (fun (MatchCase (Identifier label, meta, body)) ->
                                                                  let types' = match meta with
                                                                               | Some (Identifier meta') -> types.Add(meta', metaTypeForLabel types label)
                                                                               | None -> types             
                                                                  MatchCase (Identifier label, meta, visitor types' body))
                                                               cases)
  | Seq (expr1, expr2) -> Seq (visitor types expr1, visitor types expr2)
  | Record fields -> Record (List.map (fun (n, e) -> (n, visitor types e)) fields)
  | RecordWith (source, newFields) -> RecordWith (visitor types source, List.map (fun (n, e) -> (n, visitor types e)) newFields)
  | Time (length, unit) -> Time (visitor types length, unit)
  | Id _ | Integer _ | String _ | Bool _ | SymbolExpr _ | Null -> expr  


type ExprsContext = Map<string, expr>

let rec lookupExpr var (varExprs:ExprsContext) =
  match varExprs.[var] with
  | Id (Identifier var') -> lookupExpr var' varExprs
  | other -> other


(*
 * Rewrites entity declarations into groupby operations.
 * 
 * Example:
 *
 * entity Room =
 *   createFrom(temp_readings, :room_id)
 *   hasMany :products
 *
 * entity Product =
 *   createFrom (entries, :product_id)
 *   belongsTo :room
 *   member self.temperature = self.room.temperature;;
 *
 * is translated into:
 *
 * $Room_all = temp_readings
 *               .groupby(:room_id, fun g -> let %room_id     = g.last(:room_id) in
 *                                           let %temperature = g.last(:temperature) in
 *                                           let %events      = g
 *                                           let %products    = $Product_all.where (fun p -> p.room_id == %room_id)
 *                                                                          .select (fun p -> $ref (p)) in
 *                                           { room_id     = %room_id,
 *                                             temperature = %temperature
 *                                             events      = %events
 *                                             products    = %products }  
 *
 * $Product_all = entries
 *                  .groupby(:product_id, fun g -> let %product_id  = g.last(:product_id) in
 *                                                 let %room_id     = g.last(:room_id) in
 *                                                 let %events      = g
 *                                                 let %room        = $ref(Room.all[%room_id]) in
 *                                                 let %temperature = %room.temperature
 *                                                 { product_id  = %product_id,
 *                                                   room_id     = %room_id,
 *                                                   events      = %events
 *                                                   room        = %room,
 *                                                   temperature = %temperature })
 *
 * "entities" collects the names of declared entities.
 *)
let rec transEntities (entities:Set<string>) (types:TypeContext) = function
  | DefVariant _ as def -> entities, def
  | Def (Identifier name, expr, None) -> entities, Def (Identifier name, transDictAll entities expr, None)
  | Expr expr -> entities, Expr (transDictAll entities expr)
  | StreamDef _ as def -> entities, def
  | Entity (Identifier name, ((source, Symbol uniqueId), assocs, members)) as expr ->
      let streamFields = match typeOf types source with
                         | TyStream (TyRecord f) -> f
                         | _ -> failwith "can't happen!"
      // Translate the fields inherited from the stream                         
      let streamFields = [ for pair in streamFields ->
                             (pair.Key, MethodCall (Id (Identifier "g"), Identifier "last",
                                                    [SymbolExpr (Symbol pair.Key)]))]

      // self.events
      let autoGenFields = streamFields @ ["events", Id (Identifier "g")]

      // Translate associations                                                                
      let assocFields = List.fold (fun acc assoc ->
                                     match assoc with
                                     | BelongsTo (Symbol entity) -> 
                                         let entityIdExpr = Id (Identifier (sprintf ":%s_id" entity))
                                         let indexEntity = ArrayIndex (Id (Identifier (entityDict (String.capitalize entity))), entityIdExpr)
                                         let fieldExpr = FuncCall(Id (Identifier "$ref"), [indexEntity])
                                         acc @ [entity, fieldExpr]
                                     | HasMany (Symbol entity) ->
                                         let entityName = String.capitalize entity |> String.singular
                                         let entityId = sprintf "%s_id" (name.ToLower())
                                         let x = Identifier "x"
                                         let filter = Lambda ([Param (x, None)], BinaryExpr (Equal, MemberAccess (Id x, Identifier entityId), Id (Identifier (":" + entityId))))
                                         let whereExpr = MethodCall (Id (Identifier (entityDict entityName)), Identifier "where", [filter])
                                         let toRef = Lambda ([Param (x, None)], FuncCall (Id (Identifier "$ref"), [Id x]))
                                         let selectRef = MethodCall (whereExpr, Identifier "select", [toRef])
                                         acc @ [entity, selectRef])
                                   autoGenFields assocs
                                      
      // Translate additional member declarations.                                      
      let allFields = List.fold (fun acc (Member (self, Identifier name, expr, listeners)) ->
                                   let expr' = transDictAll entities expr
                                   let expr'' = transSelf expr' self
                                   acc @ [name, expr''])
                                assocFields members

      

      // Create let's for all fields
      let record = List.map (fun (k, v) -> (k, Id (Identifier (sprintf ":%s" k)))) allFields
      let fieldDecls = List.foldBack (fun (k, v) acc -> Let (Identifier (sprintf ":%s" k), None, v, acc)) allFields (Record record)

      //let record = Record (Map.to_list allFields)
      let groupByExpr = MethodCall(source, Identifier "groupby",
                                   [SymbolExpr (Symbol uniqueId); Lambda ([Param (Identifier "g", None)], fieldDecls)])
      let assign = Def (Identifier (entityDict name), groupByExpr, None)                 
      entities.Add(name), assign
  | _ -> failwithf "Won't happen because function translations happen first."

(* Replaces Entity.all with $Entity_all, everywhere *)
and transDictAll entities expr =
  let rec replacer expr = match expr with
                          | MemberAccess (Id (Identifier target), Identifier "all") when Set.contains target entities -> (Id (Identifier (entityDict target)))
                          | _ -> visit expr replacer
  replacer expr

(* Replaces self.field with the generated variable that contains the value of the field *)
and transSelf expr self =
  let rec replacer expr = match expr with
                          | MemberAccess (Id self', Identifier field) when self = self' -> Id (Identifier (sprintf ":%s" field))
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
  | StreamDef _ as def -> varExprs, def
  | _ -> failwithf "Won't happen because entity and function translations happens first."


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


(* Translate listeners into calls to listenN
 *
 * x = 0
 *   | when ev in temp_readings -> x + ev.temperature
 *   | when ev in hum_readings if ev.humidity > 30 -> -1
 *
 *  => x = listenN (0, when (temp_readings, fun ev -> fun def -> def + ev.temperature),
 *                     when (hum_readings, fun ev -> fun def -> if ev.humidity > 30 then -1 else def))
 *)
let rec transListeners types stmt =
  let rec replacer self field expr =
    match expr with
    | MemberAccess (Id self', Identifier field') when field = field' && self' = self -> Id (Identifier field)
    | Let (self', _, _, _) when self = self' -> expr
    | Lambda (args, _) when List.exists (fun (Param (self', _)) -> self = self') args -> expr
    | _ -> visit expr (replacer self field)

  let transListener def defType replaceInBody (Listener (evOpt, stream, guardOpt, body)) =
    let body' = replaceInBody def body
    let body'' = match guardOpt with
                 | None -> body'
                 | Some expr -> If (expr, body', Id (Identifier def))
    let ev = match evOpt with
             | Some (Identifier ev) -> ev
             | None -> "$ev"
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
                         let types' =  types.Add(self, types.[ename])
                         let memberType = typeOf types' expr
                         let whens = List.map (transListener name memberType (replacer (Identifier self))) listeners
                         let expr' = FuncCall(Id (Identifier "listenN"), expr::whens)
                         members @ [Member (Identifier self, Identifier name, expr', None)])
                  [] members
      Entity (Identifier ename, (createFrom, assocs, members'))
  | _ -> stmt


(* Replaces {target.member}.other[...] *)     
let rec transFixedAccesses (varExprs:Map<string, expr>) (types:TypeContext) stmt =
  let rec splitFixedSegment expr = 
    match expr with
    | MemberAccess (target, (Identifier name)) ->
        let fixd, rest = splitFixedSegment target
        fixd, rest @ [name]
    | FixedAccess expr -> expr, []
    | Id (Identifier ident) -> expr, [] 

  let rec replacer (types:TypeContext) expr =
    match expr with
    | FuncCall (fn, [param]) ->
        match typeOf types param with
        | TyFixed (_, fixedExpr) ->
            let fixd, rest = splitFixedSegment fixedExpr
            match typeOf types fixd with
            | TyRef (TyAlias entityType) as typ ->
                let entityType', entityIdField =
                  match types.[entityType] with
                  | TyType (name, _, id) -> name, id
                  | _ -> failwithf "The referenced entity is not a TyType? Can't happen."
                let typeDict = entityDict entityType
                let memberAccess = List.fold (fun acc attr -> MemberAccess (acc, Identifier attr)) (Id (Identifier "$o")) rest
                let projector = Lambda ([Param (Identifier "$o", Some typ)], FuncCall(fn, [memberAccess]))
                let dict = MethodCall (Id (Identifier typeDict), Identifier "select", [projector])
                ArrayIndex (dict, MemberAccess (fixd, Identifier entityIdField))
            | other -> failwithf "The target of the fixed access is not an entity: %A" other
        | _ -> expr
    | ArrayIndex (target, (Time _ as time)) ->
        match typeOf types target with
        | TyFixed (_, fixedExpr) ->
            let fixd, rest = splitFixedSegment fixedExpr
            match typeOf types fixd with
            | TyRef (TyAlias entityType) as typ ->
                let entityType', entityIdField =
                  match types.[entityType] with
                  | TyType (name, _, id) -> name, id
                  | _ -> failwithf "The referenced entity is not a TyType? Can't happen."
                let typeDict = entityDict entityType
                let memberAccess = List.fold (fun acc attr -> MemberAccess (acc, Identifier attr)) (Id (Identifier "$o")) rest
                let projector = Lambda ([Param (Identifier "$o", Some typ)], ArrayIndex (memberAccess, time))
                let dict = MethodCall (Id (Identifier typeDict), Identifier "select", [projector])
                ArrayIndex (dict, MemberAccess (fixd, Identifier entityIdField))
            | other -> failwithf "The target of the fixed access is not an entity: %A" other
        | _ -> expr
    | _ -> visitWithTypes types expr replacer

  match stmt with
  | DefVariant _ as expr -> varExprs, expr
  | Def (Identifier name, expr, None) ->
      let expr' = replacer types expr
      varExprs.Add(name, expr'), Def (Identifier name, expr', None)
  | Expr expr -> varExprs, Expr (replacer types expr)
  | StreamDef _ as def -> varExprs, def
  | _ -> failwithf "Won't happen because everything else is translated first."


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
    List.fold (fun (entities, stmts) stmt ->
                 let entities, stmt' = transEntities entities types stmt
                 entities, stmts @ [stmt'])
              (Set.empty, []) stmts2

  let _, stmts4 =
    List.fold (fun (varExprs, stmts) stmt ->
                 let varExprs', stmt' = transRecordWith varExprs stmt
                 varExprs', stmts @ [stmt'])
              (Map.empty, []) stmts3
  
  let _, stmts5 =
    List.fold (fun (varExprs, stmts) stmt ->
                 let varExprs', stmt' = transFixedAccesses varExprs types stmt
                 varExprs', stmts @ [stmt'])
              (Map.empty, []) stmts4
                               
  stmts5
  
      