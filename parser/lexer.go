package parser

import (
	"bufio"
	"io"
)

const (
	EOF  = iota
	SEMI // ;

	AND // &&
	OR  // ||

	STRING_TYPE // string
	BOOL_TYPE   // bool
	INT_TYPE    // int

	STRING // string
	INT    // int
	BOOL   // bool

	PRIMARY // primary

	COMMA // COMMA

	ASSIGN // =
)

type Position struct {
	line   int
	column int
}

type Lexer struct {
	pos    Position
	reader *bufio.Reader
}

func NewLexer(reader io.Reader) *Lexer {
	return &Lexer{
		pos:    Position{line: 1, column: 0},
		reader: bufio.NewReader(reader),
	}
}

func (l *Lexer) Lex() (Position, Token, string) {
	for {
		r, _, err := l.reader.ReadRune()
		if err != nil {
			if err == io.EOF {
				return l.pos, EOF, ""
			}

			panic(err)
		}

		l.pos.column++

		switch r {
		case '\\':
			l.lexString()
		}
	}
}

func (l *Lexer) lexString() string {
	var lit string
	for {
		r, _, err := l.reader.ReadRune()
	}
}
