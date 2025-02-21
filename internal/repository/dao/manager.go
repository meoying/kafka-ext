package dao

import (
	"fmt"
	"gorm.io/gorm"
)

type GormManager struct {
	dbs map[string]*gorm.DB
}

func NewGormManager(dbs map[string]*gorm.DB) *GormManager {
	return &GormManager{dbs: dbs}
}

func (g *GormManager) CreateDAO(dbName string) (MessageDAO, error) {
	db, ok := g.dbs[dbName]
	if !ok {
		return nil, fmt.Errorf("not found db name %s", dbName)
	}
	return NewMsgDAO(db), nil
}
