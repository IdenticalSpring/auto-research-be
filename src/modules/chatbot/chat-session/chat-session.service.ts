import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma.service';
import { getAiModelConfig } from '../../../common/kafka/kafka.constants';
import { CreateChatSessionDto } from './dto/create-chat-session.dto';
import { UpdateChatSessionDto } from './dto/update-chat-session.dto';

@Injectable()
export class ChatSessionService {
  constructor(private readonly prisma: PrismaService) {}

  async create(userId: string, dto: CreateChatSessionDto) {
    await this.ensureUser(userId);
    if (dto.documentId) await this.ensureDocument(userId, dto.documentId);
    return this.prisma.chatSession.create({
      data: { ...dto, model: getAiModelConfig().defaultModel, userId },
    });
  }

  findAll(userId: string) {
    return this.prisma.chatSession.findMany({
      where: { userId },
      include: { messages: true },
      orderBy: { updatedAt: 'desc' },
    });
  }

  async findOne(userId: string, id: string) {
    const session = await this.prisma.chatSession.findUnique({
      where: { id, userId },
      include: { messages: true },
    });
    if (!session) throw new NotFoundException('Chat session not found');
    return session;
  }

  async update(userId: string, id: string, dto: UpdateChatSessionDto) {
    await this.findOne(userId, id);
    if (dto.documentId) await this.ensureDocument(userId, dto.documentId);
    // Luôn ghi model đang cấu hình để session cũ không pin model đã bị thay thế.
    return this.prisma.chatSession.update({
      where: { id, userId },
      data: { ...dto, model: getAiModelConfig().defaultModel },
    });
  }

  async remove(userId: string, id: string) {
    await this.findOne(userId, id);
    await this.prisma.chatSession.delete({ where: { id, userId } });
    return { message: 'Chat session deleted successfully' };
  }

  private async ensureUser(userId: string) {
    const user = await this.prisma.user.findUnique({
      where: { id: userId },
      select: { id: true },
    });
    if (!user) throw new NotFoundException('User not found');
  }

  private async ensureDocument(userId: string, documentId: string) {
    const document = await this.prisma.document.findUnique({
      where: { id: documentId, userId },
      select: { id: true },
    });
    if (!document) throw new NotFoundException('Document not found');
  }
}
