package dao

import (
	"fmt"
	"gorm.io/gorm"
)

type GormCreator struct {
	dbs map[string]*gorm.DB
}

func NewGormCreator(dbs map[string]*gorm.DB) *GormCreator {
	return &GormCreator{dbs: dbs}
}

func (g *GormCreator) Create(dbName string) (MessageDAO, error) {
	db, ok := g.dbs[dbName]
	if !ok {
		return nil, fmt.Errorf("not found db name %s", dbName)
	}
	return NewMsgDAO(db), nil
}
