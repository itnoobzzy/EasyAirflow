from datetime import datetime
import logging

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declared_attr

logger = logging.getLogger(__name__)


class DeletionMixin(object):
    deletion = sa.Column(sa.Boolean, default=False, comment='软删除标志')

    @property
    def is_deletion(self):
        return self.deletion


class AuditMixinNullable(object):
    """
    主要提供created_on/created_by等列
    """
    ctime = sa.Column(sa.DateTime, default=datetime.now, nullable=True, comment='创建时间')
    mtime = sa.Column(sa.DateTime, default=datetime.now, onupdate=datetime.now, nullable=True, comment='修改时间')

    @declared_attr
    def created_by_fk(self):
        return sa.Column(
            sa.Integer,
            nullable=True,
            comment='创建者'
        )

    @declared_attr
    def changed_by_fk(cls):
        return sa.Column(
            sa.Integer,
            nullable=True,
            comment='修改者'
        )