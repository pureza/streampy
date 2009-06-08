type prog =
  | Expr of expr
  | Def of id * expr
  | Entity of id * (createFrom * association list * attribute list)

and expr =
  | Let of id * Type option * expr * expr
  | BinaryExpr of op * expr * expr
  | MethodCall of expr * id * expr list
  | FuncCall of expr * expr list
  | MemberAccess of expr * id
  | Lambda of param list * expr
  | If of expr * expr * expr
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

and symbol = Symbol of string

and createFrom = expr * symbol
and association =
  | BelongsTo of symbol
  | HasMany of symbol
and attribute = Member of id * id * expr  

and Type =
  | TyUnit
  | TyBool
  | TyInt
  | TyString
  | TySymbol
  | TyType of Map<string, Type> * string (* string is the field that gives the unique id *)
  | TyArrow of Type * Type
  //| TyLambda of List<string * Type> * Type
  | TyEntity of string
  | TyRecord of Map<string, Type>
  | TyStream of Type
  | TyWindow of Type * WindowType
  | TyDict of Type
  | TyRef of Type
  
  override self.ToString() =
    match self with
    | TyUnit -> "()"
    | TyBool -> "bool"
    | TyInt -> "int"
    | TyString -> "string"
    | TySymbol -> "symbol"
    | TyType _ -> "type"
    | TyArrow (type1, type2) -> sprintf "%O -> %O" type1 type2
   // | TyLambda _ -> "lambda"
    | TyEntity typ -> sprintf "instanceOf %O" typ
    | TyRecord _ -> "record"
    | TyStream _ -> "stream"
    | TyWindow _ -> "window"
    | TyDict _ -> "dict"
    | TyRef t -> sprintf "ref<%O>" t

and WindowType =
  | TimedWindow of int    

