type prog =
  | Expr of expr
  | DefVariant of id * (id * Type) list
  | Def of id * expr * option<listener list>
  | Entity of id * (createFrom * association list * attribute list)
  | Function of id * param list * Type * expr

and expr =
  | Let of id * Type option * expr * expr
  | BinaryExpr of op * expr * expr
  | MethodCall of expr * id * expr list
  | FuncCall of expr * expr list
  | MemberAccess of expr * id
  | Lambda of param list * expr
  | If of expr * expr * expr
  | Match of expr * matchCase list
  | ArrayIndex of expr * expr
  | Seq of expr * expr
  | Record of (string * expr) list
  | RecordWith of expr * (string * expr) list
  | Integer of int
  | Bool of bool
  | String of string
  | Time of expr * timeUnit
  | SymbolExpr of symbol
  | Id of id
  
  member self.Name =
    match self with
    | Let (Identifier id, _, _, _) -> sprintf "let %s = ... in" id
    | BinaryExpr (op, _, _) -> sprintf "BinaryExpr(%A)" op
    | MethodCall (target, Identifier name, _) -> sprintf "%s.%s(...)" target.Name name
    | FuncCall (func, _) -> sprintf "%s(...)" func.Name
    | MemberAccess _ -> "MemberAccess"
    | Lambda _ -> "Lambda"
    | If _ -> "if"
    | Match _ -> "match"
    | ArrayIndex _ -> "ArrayIndex"
    | Seq (expr1, expr2) -> sprintf "%s; %s" expr1.Name expr2.Name
    | Record _ -> "Record"
    | RecordWith _ -> "RecordWith"
    | Integer i -> i.ToString()
    | Bool b -> b.ToString()
    | String s -> sprintf "\"%O\"" s
    | Time _ -> "Time"
    | SymbolExpr (Symbol sym) -> sprintf ":%s" sym
    | Id (Identifier id) -> id

and op =
  | GreaterThan
  | GreaterThanOrEqual
  | Equal
  | NotEqual
  | LessThanOrEqual
  | LessThan
  | And
  | Or
  | Plus
  | Minus
  | Times
  | Div
  | Mod

and timeUnit = Min | Sec

and id = Identifier of string

and param = Param of id * Type option

and matchCase = MatchCase of id * id option * expr // A(v) -> expr or A -> expr

and symbol = Symbol of string

and createFrom = expr * symbol
and association =
  | BelongsTo of symbol
  | HasMany of symbol
and attribute = Member of id * id * expr * option<listener list>

and listener = Listener of id option * expr option * expr option * expr

and Type =
  | TyUnit
  | TyBool
  | TyInt
  | TyString
  | TySymbol
  | TyType of string * Map<string, Type> * string (* name * fields * unique id field *)
  | TyArrow of Type * Type
  //| TyEntity of string
  | TyAlias of string
  | TyRecord of Map<string, Type>
  | TyStream of Type
  | TyWindow of Type * WindowType
  | TyDict of Type
  | TyRef of Type
  | TyVariant of id * (id * Type) list
  | TyUnknown of Type
  
  override self.ToString() =
    match self with
    | TyUnit -> "unit"
    | TyBool -> "bool"
    | TyInt -> "int"
    | TyString -> "string"
    | TySymbol -> "symbol"
    | TyType _ -> "type"
    | TyArrow (type1, type2) -> sprintf "%O -> %O" type1 type2
    //| TyEntity typ -> sprintf "instanceOf %O" typ
    | TyAlias typ -> sprintf "alias<%s>" typ
    | TyRecord _ -> "record"
    | TyStream _ -> "stream"
    | TyWindow _ -> "window"
    | TyDict _ -> "dict"
    | TyRef t -> sprintf "ref<%O>" t
    | TyVariant (Identifier id, cases) -> sprintf "%s<%s>" id (List.fold (fun acc (Identifier name, _) -> acc + "|" + name) "" cases)
    | TyUnknown t -> sprintf "unk<%O>" t
    
  member self.IsUnknown () =
    match self with
    | TyUnknown _ -> true
    | _ -> false

and WindowType =
  | TimedWindow of int    


let freeVars expr =
  let rec freeVars' boundVars expr =
    match expr with
    | MethodCall (target, (Identifier name), paramExps) ->
        List.fold (fun acc paramExpr -> Set.union acc (freeVars' boundVars paramExpr))
                  (freeVars' boundVars target) paramExps
    | ArrayIndex (target, index) ->
        Set.union (freeVars' boundVars target) (freeVars' boundVars index)
    | FuncCall (fn, paramExps) ->
        List.fold (fun acc paramExpr -> Set.union acc (freeVars' boundVars paramExpr))
                  (freeVars' boundVars fn) paramExps
    | MemberAccess (target, (Identifier name)) -> freeVars' boundVars target
    | Record fields ->
        List.fold (fun acc (_, fieldExpr) -> Set.union acc (freeVars' boundVars fieldExpr))
                  Set.empty fields
    | RecordWith (record, fields) ->
        List.fold (fun acc (_, fieldExpr) -> Set.union acc (freeVars' boundVars fieldExpr))
                  (freeVars' boundVars record) fields
    | Lambda (args, body) ->
        let boundVars' = Set.union (List.fold (fun acc (Param (Identifier name, _)) -> Set.add name acc) Set.empty args) boundVars
        freeVars' boundVars' body
    | Let (Identifier name, optType, binder, body) ->
        let freeBinder = freeVars' boundVars binder
        let boundVars' = Set.add name boundVars
        Set.union freeBinder (freeVars' boundVars' body)
    | If (cond, thn, els) -> Set.union (freeVars' boundVars cond) (Set.union (freeVars' boundVars thn) (freeVars' boundVars els))
    | Match (expr, cases) ->
        let freeExpr = freeVars' boundVars expr
        List.fold (fun acc (MatchCase (_, metaOpt, body)) ->
                     let freeCase =
                       match metaOpt with
                       | Some (Identifier meta) -> freeVars' (boundVars.Add(meta)) body
                       | None -> freeVars' boundVars body
                     Set.union acc freeCase)
                  freeExpr cases
    | BinaryExpr (oper, expr1, expr2) as expr -> Set.union (freeVars' boundVars expr1) (freeVars' boundVars expr2)
    | Seq (expr1, expr2) -> Set.union (freeVars' boundVars expr1) (freeVars' boundVars expr2)
    | Id (Identifier name) -> if Set.contains name boundVars then Set.empty else Set.singleton name
    | Time _ -> Set.empty
    | Integer i -> Set.empty
    | String s -> Set.empty
    | SymbolExpr _ -> Set.empty
    | Bool b -> Set.empty
    
  freeVars' Set.empty expr
  

let isRecursive name expr =
  match expr with
  | Lambda (args, body) -> Set.contains name (freeVars expr)
  | _ -> false
  
