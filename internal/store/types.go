package store

type insertFormat string

const (
	FormatJSON      insertFormat = "JSONEachRow"
	FormatRowBinary insertFormat = "RowBinaryWithNamesAndTypes"
)

type Policy string

const (
	PolicyReplace  Policy = "replace"
	PolicyTruncate Policy = "truncate"
	PolicyFail     Policy = "fail"
	PolicyAppend   Policy = "append"
)

type DatabaseTable struct {
	Database string
	Table    string
}
